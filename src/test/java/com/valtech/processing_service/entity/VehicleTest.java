package com.valtech.processing_service.entity;

import jakarta.validation.ConstraintViolationException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;

@DataJpaTest(properties = "spring.jpa.hibernate.ddl-auto=create-drop")
@Testcontainers
public class VehicleTest {

    @Container
    @ServiceConnection
    private static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:18.3");

    @Autowired
    private TestEntityManager testEntityManager;

    Vehicle vehicle;

    @BeforeEach
    void setUp() {
        vehicle = new Vehicle();
        vehicle.setVehicleNumber("VIN0000001");
        vehicle.setVehicleType("Audi");
        vehicle.setOwnerName("John");
        vehicle.setRegistrationDate(Instant.now());
        vehicle.setEngineNumber("E0001");
        vehicle.setStatus(VehicleStatus.PENDING);
        vehicle.setCreatedAt(Instant.now());
    }

    @Test
    void whenValidDetailsAreGiven_thenVehicleShouldGetPersisted() {
        Vehicle storedVehicle = testEntityManager.persistAndFlush(vehicle);

        Assertions.assertEquals(vehicle.getVehicleNumber(), storedVehicle.getVehicleNumber());
        Assertions.assertEquals(vehicle.getVehicleType(), storedVehicle.getVehicleType());
        Assertions.assertEquals(vehicle.getOwnerName(), storedVehicle.getOwnerName());
        Assertions.assertEquals(vehicle.getRegistrationDate(), storedVehicle.getRegistrationDate());
        Assertions.assertEquals(vehicle.getEngineNumber(), storedVehicle.getEngineNumber());
        Assertions.assertEquals(vehicle.getStatus(), storedVehicle.getStatus());
        Assertions.assertEquals(vehicle.getCreatedAt(), storedVehicle.getCreatedAt());
        Assertions.assertNotNull(storedVehicle.getId());
    }

    @Test
    void whenNullValueProvidedForVehicleNumber_throwsException() {
        vehicle.setVehicleNumber(null);
        Assertions.assertThrows(ConstraintViolationException.class, () -> {
            testEntityManager.persistAndFlush(vehicle);
        });
    }

    @Test
    void whenNullValueProvidedForOwnerName_throwsException() {
        vehicle.setOwnerName(null);
        Assertions.assertThrows(ConstraintViolationException.class, () -> {
            testEntityManager.persistAndFlush(vehicle);
        });
    }

    @Test
    void whenNullValueProvidedForCreatedAt_throwsException() {
        vehicle.setCreatedAt(null);
        Assertions.assertThrows(org.hibernate.exception.ConstraintViolationException.class, () -> {
            testEntityManager.persistAndFlush(vehicle);
        });
    }

    @Test
    void whenNullValueProvidedForVehicleType_throwsException() {
        vehicle.setVehicleType(null);
        Assertions.assertThrows(ConstraintViolationException.class, () -> {
            testEntityManager.persistAndFlush(vehicle);
        });
    }

    @Test
    void whenNullValueProvidedForVehicleStatus_throwsException() {
        vehicle.setStatus(null);
        Assertions.assertThrows(org.hibernate.exception.ConstraintViolationException.class, () -> {
            testEntityManager.persistAndFlush(vehicle);
        });
    }

    @Test
    void whenNullValueProvidedForEngineNumber_throwsException() {
        vehicle.setEngineNumber(null);
        Assertions.assertThrows(ConstraintViolationException.class, () -> {
            testEntityManager.persistAndFlush(vehicle);
        });
    }

    @Test
    void whenNullValueProvidedForRegistrationDate_throwsException() {
        vehicle.setRegistrationDate(null);
        Assertions.assertThrows(ConstraintViolationException.class, () -> {
            testEntityManager.persistAndFlush(vehicle);
        });
    }

    @Test
    void whenTryingToPersistDuplicateVehicle_throwsException() {
        testEntityManager.persistAndFlush(vehicle);

        Vehicle newVehicle = new Vehicle();
        newVehicle.setVehicleNumber("VIN0000001");
        newVehicle.setVehicleType("Audi");
        newVehicle.setOwnerName("John");
        newVehicle.setRegistrationDate(Instant.now());
        newVehicle.setEngineNumber("E0001");
        newVehicle.setStatus(VehicleStatus.PENDING);
        newVehicle.setCreatedAt(Instant.now());

        Assertions.assertThrows(org.hibernate.exception.ConstraintViolationException.class, () -> {
            testEntityManager.persistAndFlush(newVehicle);
        });
    }


}
