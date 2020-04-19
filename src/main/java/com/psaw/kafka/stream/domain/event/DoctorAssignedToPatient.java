package com.psaw.kafka.stream.domain.event;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.time.Instant;

/**
 * <p>
 * <code>{@link DoctorAssignedToPatient}</code> -
 * Notification event for Patient assignment to a doctor.
 * </p>
 */
@Getter
@ToString
public class DoctorAssignedToPatient extends Event {

    private String doctorId;
    private String patientId;
    @Setter
    private long storedTime;

    public DoctorAssignedToPatient() {
    }

    public DoctorAssignedToPatient(String eventId,
                                   String doctorId,
                                   String patientId) {
        super(eventId, Instant.now(), EventType.DOCTOR_ASSIGNMENT_NOTIFICATION);
        this.doctorId = doctorId;
        this.patientId = patientId;
    }
}
