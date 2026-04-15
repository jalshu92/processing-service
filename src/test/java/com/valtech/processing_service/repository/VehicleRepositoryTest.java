package com.valtech.processing_service.repository;

import com.valtech.processing_service.entity.Vehicle;
import com.valtech.processing_service.entity.VehicleStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.List;

@DataJpaTest(properties = "spring.jpa.hibernate.ddl-auto=create-drop")
@Testcontainers
public class VehicleRepositoryTest {

    @Container
    @ServiceConnection
    private static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:18.3");

    @Autowired
    private VehicleRepository vehicleRepository;

    @Test
    void testFindByEngineNumber_WhenEngineNumberExists_ReturnsListOfVehicle() {
        Vehicle vehicle = new Vehicle();
        vehicle.setVehicleNumber("VIN0000001");
        vehicle.setVehicleType("Audi");
        vehicle.setOwnerName("John");
        vehicle.setRegistrationDate(Instant.now());
        vehicle.setEngineNumber("E0001");
        vehicle.setStatus(VehicleStatus.PENDING);
        vehicle.setCreatedAt(Instant.now());
        vehicleRepository.save(vehicle);

        List<Vehicle> vehicles = vehicleRepository.findByEngineNumber(vehicle.getEngineNumber());

        Assertions.assertEquals(1, vehicles.size());
        Vehicle foundVehicle = vehicles.get(0);
        Assertions.assertEquals(vehicle.getVehicleNumber(), foundVehicle.getVehicleNumber());
        Assertions.assertEquals(vehicle.getVehicleType(), foundVehicle.getVehicleType());
        Assertions.assertEquals(vehicle.getOwnerName(), foundVehicle.getOwnerName());
        Assertions.assertEquals(vehicle.getRegistrationDate(), foundVehicle.getRegistrationDate());
        Assertions.assertEquals(vehicle.getEngineNumber(), foundVehicle.getEngineNumber());
        Assertions.assertEquals(vehicle.getStatus(), foundVehicle.getStatus());
        Assertions.assertEquals(vehicle.getCreatedAt(), foundVehicle.getCreatedAt());
        Assertions.assertNotNull(foundVehicle.getId());
    }

    @Test
    void testFindByEngineNumber_WhenEngineNumberDoesNotExist_ReturnsEmptyList() {
        Assertions.assertEquals(0, vehicleRepository.findByEngineNumber("E0001").size());
    }
}
