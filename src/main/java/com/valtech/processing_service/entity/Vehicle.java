package com.valtech.processing_service.entity;

import jakarta.persistence.*;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.time.Instant;

@Entity
@Table(name="vehicles")
public class Vehicle {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @NotNull(message = "Owner name is required")
    @Column(name="owner_name",  nullable = false)
    private String ownerName;

    @NotNull(message = "Vehicle number is required")
    @Size(min=10, max=10, message = "Vehicle number must be fixed length of 10")
    @Column(name="vehicle_number", nullable = false, unique = true)
    private String vehicleNumber;

    @NotNull(message = "Vehicle type is required")
    @Column(name="vehicle_type",  nullable = false)
    private String vehicleType;

    @Enumerated(EnumType.STRING)
    @Column(name="status", nullable = false)
    private VehicleStatus status;

    @Column(name="created_at", nullable = false)
    private Instant createdAt;

    @NotNull(message = "Engine number is required")
    @Column(name="engine_number", nullable = false)
    private String engineNumber;

    @NotNull(message = "Registration date is required")
    @Column(name="registration_date", nullable = false)
    private Instant registrationDate;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getOwnerName() {
        return ownerName;
    }

    public void setOwnerName(String ownerName) {
        this.ownerName = ownerName;
    }

    public String getVehicleNumber() {
        return vehicleNumber;
    }

    public void setVehicleNumber(String vehicleNumber) {
        this.vehicleNumber = vehicleNumber;
    }

    public String getVehicleType() {
        return vehicleType;
    }

    public void setVehicleType(String vehicleType) {
        this.vehicleType = vehicleType;
    }

    public VehicleStatus getStatus() {
        return status;
    }

    public void setStatus(VehicleStatus status) {
        this.status = status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public String getEngineNumber() {
        return engineNumber;
    }

    public void setEngineNumber(String engineNumber) {
        this.engineNumber = engineNumber;
    }

    public Instant getRegistrationDate() {
        return registrationDate;
    }

    public void setRegistrationDate(Instant registrationDate) {
        this.registrationDate = registrationDate;
    }

    @Override
    public String toString() {
        return "Vehicle{" +
                "id='" + id + '\'' +
                "ownerName='" + ownerName + '\'' +
                "vehicleNumber='" + vehicleNumber + '\'' +
                "vehicleType='" + vehicleType + '\'' +
                "status='" + status + '\'' +
                "createdAt='" + createdAt + '\'' +
                "engineNumber='" + engineNumber + '\'' +
                "registrationDate='" + registrationDate + '\'' +
                '}';
    }
}
