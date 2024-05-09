package com.example.iotsparkprocessor.processor;


import com.example.iotsparkprocessor.data.IoTData;
import com.example.iotsparkprocessor.data.POIData;
import com.example.iotsparkprocessor.utils.IoTDataDecoder;
import com.example.iotsparkprocessor.utils.PropertyFileReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.common.base.Optional;


import kafka.serializer.StringDecoder;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import scala.Tuple2;
import scala.Tuple3;

@Service
public class SparkProcessorComponent {
    private static final Logger logger = Logger.getLogger(SparkProcessorComponent.class);

    public void startSparkStreaming() {
        try {
            Properties prop = PropertyFileReader.readPropertyFile();
            SparkConf conf = new SparkConf()
                    .setAppName(prop.getProperty("com.iot.app.spark.app.name"))
                    .setMaster(prop.getProperty("com.iot.app.spark.master"))
                    .set("spark.cassandra.connection.host", prop.getProperty("com.iot.app.cassandra.host"))
                    .set("spark.cassandra.connection.port", prop.getProperty("com.iot.app.cassandra.port"))
                    .set("spark.cassandra.connection.keep_alive_ms", prop.getProperty("com.iot.app.cassandra.keep_alive"));
            JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
            jssc.checkpoint(prop.getProperty("com.iot.app.spark.checkpoint.dir"));

            Map<String, String> kafkaParams = new HashMap<String, String>();
            kafkaParams.put("zookeeper.connect", prop.getProperty("com.iot.app.kafka.zookeeper"));
            kafkaParams.put("metadata.broker.list", prop.getProperty("com.iot.app.kafka.brokerlist"));
            String topic = prop.getProperty("com.iot.app.kafka.topic");
            Set<String> topicsSet = new HashSet<String>();
            topicsSet.add(topic);
            JavaPairInputDStream<String, IoTData> directKafkaStream = KafkaUtils.createDirectStream(
                    jssc,
                    String.class,
                    IoTData.class,
                    StringDecoder.class,
                    IoTDataDecoder.class,
                    kafkaParams,
                    topicsSet
            );
            logger.info("Starting Stream Processing");

            JavaDStream<IoTData> nonFilteredIotDataStream = directKafkaStream.map(tuple -> tuple._2());

            JavaPairDStream<String, IoTData> iotDataPairStream = nonFilteredIotDataStream
                    .mapToPair(iot -> new Tuple2<String, IoTData>(iot.getVehicleId(), iot))
                    .reduceByKey((a, b) -> a);

            JavaMapWithStateDStream<String, IoTData, Boolean, Tuple2<IoTData, Boolean>> iotDStreamWithStatePairs =
                    iotDataPairStream.mapWithState(StateSpec.function(processedVehicleFunc).timeout(Durations.seconds(3600)));//maintain state for one hour

            JavaDStream<Tuple2<IoTData, Boolean>> filteredIotDStreams = iotDStreamWithStatePairs
                    .map(tuple2 -> tuple2)
                    .filter(tuple -> tuple._2.equals(Boolean.FALSE));

            JavaDStream<IoTData> filteredIotDataStream = filteredIotDStreams.map(tuple -> tuple._1);

            filteredIotDataStream.cache();

            IoTTrafficDataProcessor iotTrafficProcessor = new IoTTrafficDataProcessor();
            iotTrafficProcessor.processTotalTrafficData(filteredIotDataStream);
            iotTrafficProcessor.processWindowTrafficData(filteredIotDataStream);

            POIData poiData = new POIData();
            poiData.setLatitude(33.877495);
            poiData.setLongitude(-95.50238);
            poiData.setRadius(30);//30 km

            Broadcast<Tuple3<POIData, String, String>> broadcastPOIValues = jssc.sparkContext()
                    .broadcast(new Tuple3<>(poiData, "Route-37", "Truck"));
            iotTrafficProcessor.processPOIData(nonFilteredIotDataStream, broadcastPOIValues);

            System.out.println("aziz");
            jssc.start();
            jssc.awaitTermination();
        } catch (Exception e) {
            logger.error("Error in Spark processing", e);
        }
    }

    private static final Function3<String, Optional<IoTData>, State<Boolean>, Tuple2<IoTData, Boolean>> processedVehicleFunc =
            (String, iot, state) -> {
                Tuple2<IoTData, Boolean> vehicle = new Tuple2<>(iot.get(), false);
                if (state.exists()) {
                    vehicle = new Tuple2<>(iot.get(), true);
                } else {
                    state.update(Boolean.TRUE);
                }
                return vehicle;
            };
}
