package com.psaw.kafka.stream.domain.entity;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.time.Instant;

/**
 * <p>
 * <code>Patient</code> - Patient domain entity
 * </p>
 */
@ToString
@Getter
public class Patient implements Serializable {
    private String id;
    private int age;
    private String name;
    private String address;
    @Setter
    private String doctorId;
    @Setter
    private Instant lastUpdatedTime;
}
