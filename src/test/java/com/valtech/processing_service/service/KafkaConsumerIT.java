package com.valtech.processing_service.service;

import com.valtech.processing_service.entity.Vehicle;
import com.valtech.processing_service.entity.VehicleStatus;
import com.valtech.processing_service.repository.VehicleRepository;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.orm.jpa.AutoConfigureTestEntityManager;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

@SpringBootTest(properties = "spring.jpa.hibernate.ddl-auto=create-drop")
@Testcontainers
public class KafkaConsumerIT {

    @Autowired
    private KafkaTemplate<String, Vehicle> kafkaTemplate;

    @Container
    @ServiceConnection
    private static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:18.3");

    @Container
    @ServiceConnection
    private static KafkaContainer kafka = new KafkaContainer("apache/kafka-native:4.2.0");

    @MockitoSpyBean
    private VehicleService vehicleService;

    @Autowired
    private VehicleRepository vehicleRepository;

    @MockitoSpyBean
    private KafkaProducer kafkaProducer;

    @Value("${registration.topic}")
    private String registrationTopic;

    private Vehicle vehicle;

    @BeforeEach
    public void setup() {
        vehicleRepository.deleteAll();
        vehicle = new Vehicle();
        vehicle.setVehicleNumber("VIN0000001");
        vehicle.setVehicleType("Audi");
        vehicle.setOwnerName("John");
        vehicle.setRegistrationDate(Instant.now());
        vehicle.setEngineNumber("E0001");
        vehicle.setStatus(VehicleStatus.PENDING);
        vehicle.setCreatedAt(Instant.now());

        vehicleRepository.save(vehicle);
    }

    @Test
    void testKafkaConsumer_WhenEngineNumberDoesNotExist_DoNothing(){
        vehicle.setEngineNumber("E0002");
        kafkaTemplate.send(registrationTopic, vehicle);

        await()
                .pollInterval(Duration.ofSeconds(3))
                .atMost(10, SECONDS)
                .untilAsserted(() -> {
                    verify(vehicleService, Mockito.atLeastOnce()).findByEngineNumber(vehicle.getEngineNumber());
                });
        verify(vehicleService, Mockito.never()).saveVehicle(any(Vehicle.class));
        verify(kafkaProducer, Mockito.never()).sendMessage(any(Vehicle.class));
    }

    @Test
    void testKafkaConsumer_WhenMultipleVehiclesExistWithSameEngineNumber_SaveStatusAsError() {
        Vehicle newVehicle = new Vehicle();
        newVehicle.setVehicleNumber("VIN0000002");
        newVehicle.setVehicleType("Porsche");
        newVehicle.setOwnerName("Jalaj");
        newVehicle.setRegistrationDate(Instant.now());
        newVehicle.setEngineNumber(vehicle.getEngineNumber());
        newVehicle.setStatus(VehicleStatus.PENDING);
        newVehicle.setCreatedAt(Instant.now());

        vehicleRepository.save(newVehicle);
        kafkaTemplate.send(registrationTopic, newVehicle);

        await()
                .pollInterval(Duration.ofSeconds(3))
                .atMost(10, SECONDS)
                .untilAsserted(() -> {
                    verify(vehicleService, Mockito.atLeastOnce()).findByEngineNumber(vehicle.getEngineNumber());
                    verify(vehicleService, Mockito.atLeastOnce()).saveVehicle(any(Vehicle.class));
                });
        Vehicle updatedVehicle = vehicleRepository.findById(newVehicle.getId()).orElse(null);
        Assertions.assertNotNull(updatedVehicle);
        Assertions.assertEquals(VehicleStatus.ERROR, updatedVehicle.getStatus());
        verify(kafkaProducer, Mockito.never()).sendMessage(any(Vehicle.class));
    }

    @Test
    void testKafkaConsumer_WhenVehicleIsRegisteredMoreThanWeekAgo_SaveStatusAsError() {
        Vehicle newVehicle = new Vehicle();
        newVehicle.setVehicleNumber("VIN0000002");
        newVehicle.setVehicleType("Porsche");
        newVehicle.setOwnerName("Jalaj");
        newVehicle.setRegistrationDate(Instant.now().minus(Duration.ofDays(7)).minus(Duration.ofMinutes(5)));
        newVehicle.setEngineNumber("E0002");
        newVehicle.setStatus(VehicleStatus.PENDING);
        newVehicle.setCreatedAt(Instant.now());

        vehicleRepository.save(newVehicle);
        kafkaTemplate.send(registrationTopic, newVehicle);

        await()
                .pollInterval(Duration.ofSeconds(3))
                .atMost(10, SECONDS)
                .untilAsserted(() -> {
                    verify(vehicleService, Mockito.atLeastOnce()).findByEngineNumber(newVehicle.getEngineNumber());
                    verify(vehicleService, Mockito.atLeastOnce()).saveVehicle(any(Vehicle.class));
                });
        Vehicle updatedVehicle = vehicleRepository.findById(newVehicle.getId()).orElse(null);
        Assertions.assertNotNull(updatedVehicle);
        Assertions.assertEquals(VehicleStatus.ERROR, updatedVehicle.getStatus());
        verify(kafkaProducer, Mockito.never()).sendMessage(any(Vehicle.class));
    }

    @Test
    void testKafkaConsumer_WhenRegistrationStatusIsNotPending_DoNothing() {
        Vehicle newVehicle = new Vehicle();
        newVehicle.setVehicleNumber("VIN0000002");
        newVehicle.setVehicleType("Porsche");
        newVehicle.setOwnerName("Jalaj");
        newVehicle.setRegistrationDate(Instant.now());
        newVehicle.setEngineNumber("E0002");
        newVehicle.setStatus(VehicleStatus.SUCCESS);
        newVehicle.setCreatedAt(Instant.now());

        vehicleRepository.save(newVehicle);
        kafkaTemplate.send(registrationTopic, newVehicle);

        await()
                .pollInterval(Duration.ofSeconds(3))
                .atMost(10, SECONDS)
                .untilAsserted(() -> {
                    verify(vehicleService, Mockito.atLeastOnce()).findByEngineNumber(newVehicle.getEngineNumber());
                });
        verify(vehicleService, Mockito.never()).saveVehicle(any(Vehicle.class));
        verify(kafkaProducer, Mockito.never()).sendMessage(any(Vehicle.class));
    }

    @Test
    void testKafkaConsumer_WhenValidationSucceeds_UpdateStatusToSuccessAndSendDataToNewTopic() {
        kafkaTemplate.send(registrationTopic, vehicle);

        await()
                .pollInterval(Duration.ofSeconds(3))
                .atMost(10, SECONDS)
                .untilAsserted(() -> {
                    verify(vehicleService, Mockito.atLeastOnce()).findByEngineNumber(vehicle.getEngineNumber());
                    verify(vehicleService, Mockito.atLeastOnce()).saveVehicle(any(Vehicle.class));
                    verify(kafkaProducer, Mockito.atLeastOnce()).sendMessage(any(Vehicle.class));
                });
        Vehicle updatedVehicle = vehicleRepository.findById(vehicle.getId()).orElse(null);
        Assertions.assertNotNull(updatedVehicle);
        Assertions.assertEquals(VehicleStatus.SUCCESS, updatedVehicle.getStatus());

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(
                kafka.getBootstrapServers(), "test-group", "true");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");

        Consumer<String, Object> consumer = new DefaultKafkaConsumerFactory<>(
                consumerProps, new StringDeserializer(), new JsonDeserializer<>()).createConsumer();
        consumer.subscribe(Collections.singletonList("vehicle-registration-status"));

        ConsumerRecord<String, Object> record = KafkaTestUtils.getSingleRecord(consumer, "vehicle-registration-status");
        Vehicle consumedVehicle = (Vehicle) record.value();
        Assertions.assertEquals(updatedVehicle.getVehicleNumber(), consumedVehicle.getVehicleNumber());
        Assertions.assertEquals(updatedVehicle.getEngineNumber(), consumedVehicle.getEngineNumber());
        Assertions.assertEquals(updatedVehicle.getOwnerName(), consumedVehicle.getOwnerName());
        Assertions.assertEquals(updatedVehicle.getVehicleType(), consumedVehicle.getVehicleType());
        Assertions.assertEquals(updatedVehicle.getRegistrationDate(), consumedVehicle.getRegistrationDate());
        Assertions.assertEquals(VehicleStatus.SUCCESS, consumedVehicle.getStatus());
        Assertions.assertEquals(updatedVehicle.getId(), consumedVehicle.getId());
        Assertions.assertEquals(updatedVehicle.getCreatedAt(), consumedVehicle.getCreatedAt());
    }

}
