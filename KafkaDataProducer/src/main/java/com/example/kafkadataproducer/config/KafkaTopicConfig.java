package com.example.kafkadataproducer.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaTopicConfig {
    @Value(value = "${spring.kafka.producer.bootstrap-servers}")
    private String bootstrapAddress;

    /**
     * Creates a KafkaAdmin bean for managing Kafka topics.
     *
     * @return The KafkaAdmin instance.
     */
    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        return new KafkaAdmin(configs);
    }

    /**
     * Creates a NewTopic bean for creating a IotData topic.
     *
     * @param topicName         The name of the customer topic.
     * @param partitions        The number of partitions for the topic.
     * @param replicationFactor The replication factor for the topic.
     * @return NewTopic instance for the customer topic.
     */
    @Bean
    public NewTopic createIotDataTopic(@Value("${spring.kafka.topic-iot-data}") String topicName,
                                       @Value("${spring.kafka.topic-partitions}") int partitions,
                                       @Value("${spring.kafka.topic-replication-factor}") short replicationFactor) {
        return new NewTopic(topicName, partitions, replicationFactor);
    }

}
