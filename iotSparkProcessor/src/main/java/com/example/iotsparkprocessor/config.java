package com.example.iotsparkprocessor;


import com.example.iotsparkprocessor.processor.SparkProcessorComponent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

@Configuration
public class config {


    @Bean
    public SparkProcessorComponent sparkProcessorComponent() {
        SparkProcessorComponent sparkProcessorComponent = new SparkProcessorComponent();
        sparkProcessorComponent.startSparkStreaming();
        return sparkProcessorComponent;
    }
}
