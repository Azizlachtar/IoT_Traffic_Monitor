package com.example.iotsparkprocessor.processor;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.example.iotsparkprocessor.data.AggregateKey;
import com.example.iotsparkprocessor.data.IoTData;
import com.example.iotsparkprocessor.data.POIData;
import com.example.iotsparkprocessor.entity.POITrafficData;
import com.example.iotsparkprocessor.entity.TotalTrafficData;
import com.example.iotsparkprocessor.entity.WindowTrafficData;
import com.example.iotsparkprocessor.utils.GeoDistanceCalculator;
import com.google.common.base.Optional;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;
import scala.Tuple3;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static com.datastax.spark.connector.japi.CassandraStreamingJavaUtil.javaFunctions;


public class IoTTrafficDataProcessor {
    private static final Logger logger = Logger.getLogger(IoTTrafficDataProcessor.class);


    public void processTotalTrafficData(JavaDStream<IoTData> filteredIotDataStream) {

        JavaPairDStream<AggregateKey, Long> countDStreamPair = filteredIotDataStream
                .mapToPair(iot -> new Tuple2<>(new AggregateKey(iot.getRouteId(), iot.getVehicleType()), 1L))
                .reduceByKey((a, b) -> a + b);

        JavaMapWithStateDStream<AggregateKey, Long, Long, Tuple2<AggregateKey, Long>> countDStreamWithStatePair = countDStreamPair
                .mapWithState(StateSpec.function(totalSumFunc).timeout(Durations.seconds(3600)));//maintain state for one hour

        JavaDStream<Tuple2<AggregateKey, Long>> countDStream = countDStreamWithStatePair.map(tuple2 -> tuple2);
        JavaDStream<TotalTrafficData> trafficDStream = countDStream.map(totalTrafficDataFunc);

        Map<String, String> columnNameMappings = new HashMap<String, String>();
        columnNameMappings.put("routeId", "routeid");
        columnNameMappings.put("vehicleType", "vehicletype");
        columnNameMappings.put("totalCount", "totalcount");
        columnNameMappings.put("timeStamp", "timestamp");
        columnNameMappings.put("recordDate", "recorddate");

        javaFunctions(trafficDStream).writerBuilder("traffickeyspace", "total_traffic",
                CassandraJavaUtil.mapToRow(TotalTrafficData.class, columnNameMappings)).saveToCassandra();
    }


    public void processWindowTrafficData(JavaDStream<IoTData> filteredIotDataStream) {

        JavaPairDStream<AggregateKey, Long> countDStreamPair = filteredIotDataStream
                .mapToPair(iot -> new Tuple2<>(new AggregateKey(iot.getRouteId(), iot.getVehicleType()), 1L))
                .reduceByKeyAndWindow((a, b) -> a + b, Durations.seconds(30), Durations.seconds(10));

        JavaDStream<WindowTrafficData> trafficDStream = countDStreamPair.map(windowTrafficDataFunc);

        Map<String, String> columnNameMappings = new HashMap<String, String>();
        columnNameMappings.put("routeId", "routeid");
        columnNameMappings.put("vehicleType", "vehicletype");
        columnNameMappings.put("totalCount", "totalcount");
        columnNameMappings.put("timeStamp", "timestamp");
        columnNameMappings.put("recordDate", "recorddate");

        javaFunctions(trafficDStream).writerBuilder("traffickeyspace", "window_traffic",
                CassandraJavaUtil.mapToRow(WindowTrafficData.class, columnNameMappings)).saveToCassandra();
    }


    public void processPOIData(JavaDStream<IoTData> nonFilteredIotDataStream, Broadcast<Tuple3<POIData, String, String>> broadcastPOIValues) {
        System.out.println("aziz");
        JavaDStream<IoTData> iotDataStreamFiltered = nonFilteredIotDataStream
                .filter(iot -> (iot.getRouteId().equals(broadcastPOIValues.value()._2())
                        && iot.getVehicleType().contains(broadcastPOIValues.value()._3())
                        && GeoDistanceCalculator.isInPOIRadius(Double.valueOf(iot.getLatitude()),
                        Double.valueOf(iot.getLongitude()), broadcastPOIValues.value()._1().getLatitude(),
                        broadcastPOIValues.value()._1().getLongitude(),
                        broadcastPOIValues.value()._1().getRadius())));

        JavaPairDStream<IoTData, POIData> poiDStreamPair = iotDataStreamFiltered
                .mapToPair(iot -> new Tuple2<>(iot, broadcastPOIValues.value()._1()));

        JavaDStream<POITrafficData> trafficDStream = poiDStreamPair.map(poiTrafficDataFunc);

        Map<String, String> columnNameMappings = new HashMap<String, String>();
        columnNameMappings.put("vehicleId", "vehicleid");
        columnNameMappings.put("distance", "distance");
        columnNameMappings.put("vehicleType", "vehicletype");
        columnNameMappings.put("timeStamp", "timestamp");

        javaFunctions(trafficDStream)
                .writerBuilder("traffickeyspace", "poi_traffic",CassandraJavaUtil.mapToRow(POITrafficData.class, columnNameMappings))
                .withConstantTTL(120)//keeping data for 2 minutes
                .saveToCassandra();
    }

    private static final Function3<AggregateKey, Optional<Long>, State<Long>,Tuple2<AggregateKey,Long>> totalSumFunc = (key, currentSum, state) -> {
        System.out.println("aziz");
        long totalSum = currentSum.or(0L) + (state.exists() ? state.get() : 0);
        Tuple2<AggregateKey, Long> total = new Tuple2<>(key, totalSum);
        state.update(totalSum);
        return total;
    };

    private static final Function<Tuple2<AggregateKey, Long>, TotalTrafficData> totalTrafficDataFunc = (tuple -> {
        logger.debug("Total Count : " + "key " + tuple._1().getRouteId() + "-" + tuple._1().getVehicleType() + " value "+ tuple._2());
        TotalTrafficData trafficData = new TotalTrafficData();
        trafficData.setRouteId(tuple._1().getRouteId());
        trafficData.setVehicleType(tuple._1().getVehicleType());
        trafficData.setTotalCount(tuple._2());
        trafficData.setTimeStamp(new Date());
        trafficData.setRecordDate(new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
        return trafficData;
    });

    private static final Function<Tuple2<AggregateKey, Long>, WindowTrafficData> windowTrafficDataFunc = (tuple -> {
        logger.debug("Window Count : " + "key " + tuple._1().getRouteId() + "-" + tuple._1().getVehicleType()+ " value " + tuple._2());
        WindowTrafficData trafficData = new WindowTrafficData();
        trafficData.setRouteId(tuple._1().getRouteId());
        trafficData.setVehicleType(tuple._1().getVehicleType());
        trafficData.setTotalCount(tuple._2());
        trafficData.setTimeStamp(new Date());
        trafficData.setRecordDate(new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
        return trafficData;
    });

    private static final Function<Tuple2<IoTData, POIData>, POITrafficData> poiTrafficDataFunc = (tuple -> {
        POITrafficData poiTraffic = new POITrafficData();
        poiTraffic.setVehicleId(tuple._1.getVehicleId());
        poiTraffic.setVehicleType(tuple._1.getVehicleType());
        poiTraffic.setTimeStamp(new Date());
        double distance = GeoDistanceCalculator.getDistance(Double.valueOf(tuple._1.getLatitude()).doubleValue(),
                Double.valueOf(tuple._1.getLongitude()).doubleValue(), tuple._2.getLatitude(), tuple._2.getLongitude());
        logger.debug("Distance for " + tuple._1.getLatitude() + "," + tuple._1.getLongitude() + ","+ tuple._2.getLatitude() + "," + tuple._2.getLongitude() + " = " + distance);
        poiTraffic.setDistance(distance);
        return poiTraffic;
    });

}
