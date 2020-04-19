package com.psaw.kafka.stream.app;

import com.psaw.kafka.stream.conf.KafkaStreamConfigurationFactory;
import com.psaw.kafka.stream.domain.entity.Patient;
import com.psaw.kafka.stream.domain.event.DoctorAssignedToPatient;
import com.psaw.kafka.stream.domain.event.Event;
import com.psaw.kafka.stream.util.TopicAndStoreUtil;
import com.psaw.kafka.stream.util.serde.JsonDataSelectableSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.RecordContext;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.psaw.kafka.stream.util.TopicAndStoreUtil.getStateStoreMaterialized;
import static org.apache.kafka.common.utils.Utils.murmur2;
import static org.apache.kafka.common.utils.Utils.toPositive;

/**
 * <p>
 * <code>{@link PatientDomainAggregateUpdateAppV2}</code> -
 * Stateful notification implementation with two KTables.
 * </p>
 */
@Service
public class PatientDomainAggregateUpdateAppV2 extends AbstractPatientDomainAggregateUpdateApp {

    public PatientDomainAggregateUpdateAppV2(
            @Qualifier("application-configuration-factory") KafkaStreamConfigurationFactory configurationFactory) {
        super(configurationFactory);
    }

    private static class ValueRecordWithPartitionDetail<T extends Serializable> {
        String topic;
        String eventKey;
        String keyForPartitioning;
        T value;

        public ValueRecordWithPartitionDetail(String topic, String eventKey, String keyForPartitioning, T value) {
            this.topic = topic;
            this.eventKey = eventKey;
            this.keyForPartitioning = keyForPartitioning;
            this.value = value;
        }
    }

    @Override
    protected Topology buildStream(List<String> externalTopics) {
        String internalTopicName = appName.concat("_").concat("internal_notification_domain_entity_doctor");
        TopicAndStoreUtil.createTopic(patientAggregateTopicName, 4, (short)1, appConfiguration);
        StreamsBuilder builder = new StreamsBuilder();
        //Timestamp based version
        KTable<String, Patient> patientAggregateTable = builder.table(patientAggregateTopicName,
                Consumed.with(Serdes.String(), patientValueSerde),
                getStateStoreMaterialized("patient-store", patientValueSerde));

        KTable<String, DoctorAssignedToPatient> internalNotificationTable = builder.stream(notificationTopicName, Consumed.with(Serdes.String(), doctorAssignmentValueSerde))
                .selectKey((key, value) -> value.getPatientId())
                .through(internalTopicName, Produced
                        .with(Serdes.String(), doctorAssignmentValueSerde)
                )
                .groupByKey()
                .reduce((value1, value2) -> value2,
                        getStateStoreMaterialized("internal-notification-store", doctorAssignmentValueSerde));

        JsonDataSelectableSerializer valueDataSelectableSerializer =
                new JsonDataSelectableSerializer((Function<ValueRecordWithPartitionDetail, Serializable>) data -> data.value);

        patientAggregateTable
                .join(internalNotificationTable, new ValueJoiner<Patient, DoctorAssignedToPatient, List<ValueRecordWithPartitionDetail>>() {
                    @Override
                    public List<ValueRecordWithPartitionDetail> apply(Patient patient,
                                                                      DoctorAssignedToPatient doctorAssignedToPatient) {
                        return updatePatientWithDoctorId(patient, doctorAssignedToPatient, internalTopicName);

                    }
                })
                .toStream() // Why converting to a stream ?
                .flatMap(
                        new KeyValueMapper<String, List<ValueRecordWithPartitionDetail>, Iterable<KeyValue<String,
                                ValueRecordWithPartitionDetail>>>() {
                            @Override
                            public Iterable<KeyValue<String, ValueRecordWithPartitionDetail>> apply(String key,
                                                                                                    List<ValueRecordWithPartitionDetail> records) {
                                return records.stream()
                                        .map(new Function<ValueRecordWithPartitionDetail, KeyValue<String,
                                                ValueRecordWithPartitionDetail>>() {
                                            @Override
                                            public KeyValue<String, ValueRecordWithPartitionDetail> apply(
                                                    ValueRecordWithPartitionDetail record) {
                                                return KeyValue.pair(record.eventKey, record);
                                            }
                                        })
                                        .collect(Collectors.toList());
                            }
                        })
                .to(new TopicNameExtractor<String, ValueRecordWithPartitionDetail>() {
                    @Override
                    public String extract(String key, ValueRecordWithPartitionDetail value, RecordContext recordContext) {
                        return value.topic;
                    }
                }, Produced
                        .with(Serdes.String(), Serdes.serdeFrom(valueDataSelectableSerializer, eventDeserializer))
                        .withStreamPartitioner((topic, key, value, numPartitions) -> {
                            byte[] keyBytes = ((ValueRecordWithPartitionDetail) value).keyForPartitioning.getBytes();
                            return toPositive(murmur2(keyBytes)) % numPartitions;
                        }));

        return builder.build();
    }

    private List<ValueRecordWithPartitionDetail> updatePatientWithDoctorId(Patient patient,
                                                                           DoctorAssignedToPatient doctorAssignedToPatient,
                                                                           String internalTopicName) {
        List<ValueRecordWithPartitionDetail> valueRecordWithPartitionDetails = new ArrayList<>();
        if (patient.getLastUpdatedTime().isBefore(doctorAssignedToPatient.getCreatedTime())) {
            patient.setDoctorId(doctorAssignedToPatient.getDoctorId());
            patient.setLastUpdatedTime(Instant.now());
            ValueRecordWithPartitionDetail patientRecord = new ValueRecordWithPartitionDetail(
                    patientAggregateTopicName,
                    patient.getId(),
                    patient.getId(),
                    patient);
            ValueRecordWithPartitionDetail doctorNotificationTombstoneRecord = new ValueRecordWithPartitionDetail(
                    internalTopicName,
                    patient.getId(),
                    patient.getId(),
                    null);
            valueRecordWithPartitionDetails.add(patientRecord);
            valueRecordWithPartitionDetails.add(doctorNotificationTombstoneRecord);
        }
        return valueRecordWithPartitionDetails;
    }
}
