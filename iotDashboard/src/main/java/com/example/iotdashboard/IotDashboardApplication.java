package com.example.iotdashboard;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@ComponentScan(basePackages = {"com.example.iotdashboard.dashboard", "com.example.iotdashboard.dao"})
public class IotDashboardApplication {

	public static void main(String[] args) {
		SpringApplication.run(IotDashboardApplication.class, args);
	}

}
