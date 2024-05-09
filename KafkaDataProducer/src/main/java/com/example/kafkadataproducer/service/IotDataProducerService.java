package com.example.kafkadataproducer.service;


import com.example.kafkadataproducer.model.IoTData;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class IotDataProducerService {

    private static final Logger LOGGER= LoggerFactory.getLogger(IotDataProducerService.class);

    private final NewTopic iotDataTopic;

    private final KafkaTemplate<String, IoTData> kafkaTemplate;

    public IotDataProducerService(NewTopic iotDataTopic, KafkaTemplate<String, IoTData> kafkaTemplate) {
        this.iotDataTopic = iotDataTopic;
        this.kafkaTemplate = kafkaTemplate;
    }


    /**
     * Generates and sends IoTData events to the Kafka topic.
     */

    public void generateAndSendIoTEvents() throws InterruptedException {
        Random rand = new Random();
        LOGGER.info("Generating and sending IoT events");

        while (true) {
            List<IoTData> eventList = new ArrayList<>();

            for (int i = 0; i < 100; i++) { // create 100 vehicles
                String vehicleId = UUID.randomUUID().toString();
                String vehicleType = getRandomVehicleType(rand);
                String routeId = getRandomRoute(rand);
                Date timestamp = new Date();
                double speed = rand.nextInt(100 - 20) + 20;
                double fuelLevel = rand.nextInt(40 - 10) + 10;

                for (int j = 0; j < 5; j++) { // Add 5 events for each vehicle
                    String coords = getCoordinates(routeId);
                    String latitude = coords.substring(0, coords.indexOf(","));
                    String longitude = coords.substring(coords.indexOf(",") + 1, coords.length());
                    IoTData event = new IoTData(vehicleId, vehicleType, routeId, latitude, longitude, timestamp, speed, fuelLevel);
                    eventList.add(event);
                }
            }

            Collections.shuffle(eventList);

            for (IoTData event : eventList) {
                sendMessage(event);
                Thread.sleep(rand.nextInt(3000 - 1000) + 1000);
            }
        }
    }

    private void sendMessage(IoTData event) {
        LOGGER.info(String.format("Sending IoTData message => %s", event.toString()));

        Message<IoTData> message = MessageBuilder
                .withPayload(event)
                .setHeader(KafkaHeaders.TOPIC, iotDataTopic.name())
                .build();
        kafkaTemplate.send(message);
    }

    private String getRandomVehicleType(Random rand) {
        List<String> vehicleTypeList = Arrays.asList("Large Truck", "Small Truck", "Private Car", "Bus", "Taxi");
        return vehicleTypeList.get(rand.nextInt(5));
    }

    private String getRandomRoute(Random rand) {
        List<String> routeList = Arrays.asList("Route-37", "Route-43", "Route-82");
        return routeList.get(rand.nextInt(3));
    }

    private String getCoordinates(String routeId) {
        Random rand = new Random();
        int latPrefix = 0;
        int longPrefix = -0;
        if ("Route-37".equals(routeId)) {
            latPrefix = 33;
            longPrefix = -96;
        } else if ("Route-82".equals(routeId)) {
            latPrefix = 34;
            longPrefix = -97;
        } else if ("Route-43".equals(routeId)) {
            latPrefix = 35;
            longPrefix = -98;
        }
        Float lati = latPrefix + rand.nextFloat();
        Float longi = longPrefix + rand.nextFloat();
        return lati + "," + longi;
    }



}
