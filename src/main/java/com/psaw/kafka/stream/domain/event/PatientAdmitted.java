package com.psaw.kafka.stream.domain.event;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.*;

import java.time.Instant;

/**
 * <p>
 * <code>{@link PatientAdmitted}</code> -
 * Domain event denoting a patient admission.
 * </p>
 */
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
@Getter
public class PatientAdmitted extends Event {

    private String patientId;
    private String attendedEmployeeId;
    private Instant admittedTime;

    public PatientAdmitted(String eventId,
                           Instant createdTime,
                           String patientId,
                           String attendedEmployeeId,
                           Instant admittedTime) {
        super(eventId, createdTime, EventType.REQUESTED_PATIENT_PATCH);
        this.patientId = patientId;
        this.attendedEmployeeId = attendedEmployeeId;
        this.admittedTime = admittedTime;
    }
}
