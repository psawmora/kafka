package com.psaw.kafka.stream.domain.event;

import lombok.*;

/**
 * <p>
 * <code>{@link RequestedPatientUpdate}</code> -
 * Event denoting a request to create a new Patient.
 * </p>
 */
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Getter
@ToString
public class RequestedPatientUpdate extends Event {
    private String id;
    private int age;
    private String name;
    private String address;
}
