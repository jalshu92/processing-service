package com.valtech.processing_service.service;

import com.valtech.processing_service.entity.Vehicle;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducer {

    @Value("${registration.status.topic}")
    private String topicName;

    @Autowired
    private KafkaTemplate<String, Vehicle> kafkaTemplate;

    public void sendMessage(Vehicle vehicle) {
        kafkaTemplate.send(topicName, vehicle);
    }
}
