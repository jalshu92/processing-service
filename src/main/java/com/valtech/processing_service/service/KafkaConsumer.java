package com.valtech.processing_service.service;

import com.valtech.processing_service.entity.Vehicle;
import com.valtech.processing_service.entity.VehicleStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;

@Service
public class KafkaConsumer {

    @Autowired
    private VehicleService vehicleService;

    @Autowired
    private KafkaProducer kafkaProducer;

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    @KafkaListener(topics = "${registration.topic}", groupId = "${{spring.kafka.consumer.group-id}")
    public void consume(Vehicle vehicle) {
        log.info("Consumed message: " + vehicle);
        long seconds = Instant.now().getEpochSecond() - vehicle.getRegistrationDate().getEpochSecond();
        List<Vehicle> vehicles = vehicleService.findByEngineNumber(vehicle.getEngineNumber());
        if(vehicle.getStatus() != VehicleStatus.PENDING) {
            log.error("Vehicle status maybe invalid or already processed");
            return;
        }
        else if(vehicles.isEmpty()) {
            log.error("Invalid event data. No vehicles found for engine " + vehicle.getEngineNumber());
            return;
        }
        else if(vehicles.size() > 1) {
            log.error("Found multiple vehicles with engine number " + vehicle.getEngineNumber());
            vehicle.setStatus(VehicleStatus.ERROR);
        }
        else if(seconds > 7*24*60*60) {
            log.error("Vehicle is registered more than a week ago");
            vehicle.setStatus(VehicleStatus.ERROR);
        }
        if(vehicle.getStatus().equals(VehicleStatus.ERROR)) {
            vehicleService.saveVehicle(vehicle);
        }
        else {
            vehicle.setStatus(VehicleStatus.SUCCESS);
            vehicleService.saveVehicle(vehicle);
            kafkaProducer.sendMessage(vehicle);
        }
    }
}
