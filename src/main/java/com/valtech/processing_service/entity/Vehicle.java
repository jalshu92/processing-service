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

    @Column(name="owner_name")
    private String ownerName;

    @Size(min=10, max=10, message = "Vehicle number must be fixed length of 10")
    @Column(name="vehicle_number")
    private String vehicleNumber;

    @Column(name="vehicle_type")
    private String vehicleType;

    @Enumerated(EnumType.STRING)
    @Column(name="status")
    private VehicleStatus status;

    @Column(name="created_at")
    private Instant createdAt;

    @Column(name="engine_number")
    private String engineNumber;

    @NotNull(message = "Registration date is required")
    @Column(name="registration_date")
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
