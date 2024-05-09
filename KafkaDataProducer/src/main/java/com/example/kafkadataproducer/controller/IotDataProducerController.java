package com.example.kafkadataproducer.controller;


import com.example.kafkadataproducer.service.IotDataProducerService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1")
public class IotDataProducerController {

    private final IotDataProducerService iotDataProducerService;

    public IotDataProducerController(IotDataProducerService iotDataProducerService) {
        this.iotDataProducerService = iotDataProducerService;
    }


    @PostMapping("/generateIotData")
    public ResponseEntity<String> generateIotData() {
        try {
            iotDataProducerService.generateAndSendIoTEvents();
            return new ResponseEntity<>("IoTData events generated and sent successfully", HttpStatus.OK);
        } catch (InterruptedException e) {
            return new ResponseEntity<>("Error generating and sending IoTData events", HttpStatus.INTERNAL_SERVER_ERROR);
        }

    }


}
