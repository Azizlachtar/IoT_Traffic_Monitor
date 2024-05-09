package com.example.kafkadataproducer.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Date;


@Data
@NoArgsConstructor
public class IoTData implements Serializable {

    private String vehicleId;
    private String vehicleType;
    private String routeId;
    private String longitude;
    private String latitude;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss", timezone="MST")
    private Date timestamp;
    private double speed;
    private double fuelLevel;

    public IoTData(String vehicleId, String vehicleType, String routeId, String latitude, String longitude,
                   Date timestamp, double speed, double fuelLevel) {
        super();
        this.vehicleId = vehicleId;
        this.vehicleType = vehicleType;
        this.routeId = routeId;
        this.longitude = longitude;
        this.latitude = latitude;
        this.timestamp = timestamp;
        this.speed = speed;
        this.fuelLevel = fuelLevel;
    }

}
