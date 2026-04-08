package com.valtech.processing_service.repository;

import com.valtech.processing_service.entity.Vehicle;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface VehicleRepository extends JpaRepository<Vehicle, Long> {

    List<Vehicle> findByEngineNumber(String engineNumber);
}
