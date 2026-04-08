package com.valtech.processing_service.service;

import com.valtech.processing_service.entity.Vehicle;
import com.valtech.processing_service.repository.VehicleRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class VehicleService {

    @Autowired
    private VehicleRepository vehicleRepository;

    public List<Vehicle> findByEngineNumber(String engineNumber) {
        return vehicleRepository.findByEngineNumber(engineNumber);
    }

    public Vehicle saveVehicle(Vehicle vehicle) {
        return vehicleRepository.save(vehicle);
    }
}
