package com.valtech.processing_service.service;

import com.valtech.processing_service.entity.Vehicle;
import com.valtech.processing_service.repository.VehicleRepository;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

@ExtendWith(MockitoExtension.class)
public class VehicleServiceTest {

    @Mock
    private VehicleRepository vehicleRepository;

    @InjectMocks
    private VehicleService vehicleService;

    @Test
    void findByEngineNumber() {
        Vehicle vehicle = new Vehicle();
        vehicle.setEngineNumber("1234");
        Mockito.when(vehicleRepository.findByEngineNumber("1234")).thenReturn(List.of(vehicle));
        List<Vehicle> vehicles = vehicleService.findByEngineNumber("1234");
        Assertions.assertEquals(1, vehicles.size());
        Assertions.assertEquals(vehicle, vehicles.get(0));
    }

    @Test
    void testSaveVehicle() {
        Vehicle vehicle = new Vehicle();
        Mockito.when(vehicleRepository.save(Mockito.any(Vehicle.class))).thenReturn(vehicle);
        Assertions.assertEquals(vehicle, vehicleService.saveVehicle(vehicle));
    }
}
