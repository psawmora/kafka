package com.psaw.kafka.stream.app;

import com.psaw.kafka.stream.conf.KafkaStreamConfigurationFactory;
import com.psaw.kafka.stream.domain.entity.Patient;
import com.psaw.kafka.stream.domain.event.DoctorAssignedToPatient;
import com.psaw.kafka.stream.domain.event.Event;
import com.psaw.kafka.stream.util.TopicAndStoreUtil;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;

/**
 * <p>
 * <code>{@link PatientDomainAggregateUpdateAppV1}</code> -
 * Implementation with KStream and KTable.
 * </p>
 */
@Service
public class PatientDomainAggregateUpdateAppV1 extends AbstractPatientDomainAggregateUpdateApp {

    public PatientDomainAggregateUpdateAppV1(
            @Qualifier("application-configuration-factory") KafkaStreamConfigurationFactory configurationFactory,
            @Value("${external.topics:domain_aggregate_patient, notification_domain_entity_doctor}") String[] listeningTopics) {
        super(configurationFactory);
    }

    @Override
    protected Topology buildStream(List<String> externalTopics) {
        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, Patient> patientAggregateTable = builder.table("domain_aggregate_patient",
                Consumed.with(Serdes.String(), patientValueSerde),
                TopicAndStoreUtil.getStateStoreMaterialized("patient-store", patientValueSerde));

        builder.stream("notification_domain_entity_doctor", Consumed.with(Serdes.String(), eventSerde))
                .filter((key, value) -> value.getType() == Event.EventType.DOCTOR_ASSIGNMENT_NOTIFICATION)
                .selectKey((key, event) -> ((DoctorAssignedToPatient) event).getDoctorId())
                .leftJoin(patientAggregateTable, new ValueJoiner<Event, Patient, Patient>() {
                    @Override
                    public Patient apply(Event event, Patient currentPatient) {
                        if(event.getCreatedTime().isAfter(currentPatient.getLastUpdatedTime())){
                            currentPatient.setDoctorId(((DoctorAssignedToPatient) event).getDoctorId());
                            currentPatient.setLastUpdatedTime(Instant.now());
                            return currentPatient;
                        }
                        return null;
                    }
                })
                .filter((key, value) -> value!=null)
                .to("domain_aggregate_patient", Produced.with(Serdes.String(), patientValueSerde));
        return builder.build();
    }
}
