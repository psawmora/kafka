package com.psaw.kafka.stream.domain.event;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.*;

import java.io.Serializable;
import java.time.Instant;

/**
 * <p>
 * <code>{@link Event}</code> -
 * Abstraction of a domain event.
 * </p>
 */
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public class Event implements Serializable {

    private String eventId;

    private Instant createdTime;

    private EventType type;

    public enum EventType {
        DOCTOR_ASSIGNMENT_NOTIFICATION,
        REQUESTED_PATIENT_UPDATED,
        REQUESTED_PATIENT_PATCH
    }
}
