package com.psaw.kafka.stream.app;

import com.psaw.kafka.stream.conf.KafkaStreamConfigurationFactory;
import com.psaw.kafka.stream.domain.entity.Patient;
import com.psaw.kafka.stream.domain.event.DoctorAssignedToPatient;
import com.psaw.kafka.stream.util.TopicAndStoreUtil;
import lombok.Getter;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.apache.kafka.common.utils.Utils.murmur2;
import static org.apache.kafka.common.utils.Utils.toPositive;

/**
 * <p>
 * <code>{@link PatientDomainAggregateUpdateAppV3}</code> -
 * Implementation with the processor API.
 * </p>
 */
@Service
public class PatientDomainAggregateUpdateAppV3 extends AbstractPatientDomainAggregateUpdateApp {

    public PatientDomainAggregateUpdateAppV3(
            @Qualifier("application-configuration-factory") KafkaStreamConfigurationFactory configurationFactory) {
        super(configurationFactory);
    }

    @Override
    protected Topology buildStream(List<String> externalTopics) {
        Topology topology = new Topology();

        String internalTopicName = appName.concat("_").concat("internal_notification_domain_entity_doctor");
        TopicAndStoreUtil.createTopic(patientAggregateTopicName, 4, (short)1, appConfiguration);
        topology.addSource(
                "domain_aggregate_patient_topic_source",
                Serdes.String().deserializer(),
                patientDeserializer,
                patientAggregateTopicName)
                .addSource(
                        "notification_domain_entity_doctor_topic_source",
                        Serdes.String().deserializer(), doctorAssignmentDeserializer,
                        notificationTopicName)
                .addSink(
                        "repartitioning_notification_topic_sink",
                        internalTopicName,
                        Serdes.String().serializer(), serializer,
                        new StreamPartitioner<String, DoctorAssignedToPatient>() {
                            @Override
                            public Integer partition(String topic, String key, DoctorAssignedToPatient value, int numPartitions) {
                                byte[] keyBytes = value.getPatientId().getBytes();
                                return toPositive(murmur2(keyBytes)) % numPartitions;
                            }
                        }, "notification_domain_entity_doctor_topic_source")
                .addSource("repartitioned_notification_topic_source",
                        Serdes.String().deserializer(),
                        doctorAssignmentDeserializer,
                        internalTopicName)
                .addProcessor("type_tagging_processor", TypeTaggingProcessor::new,
                        "domain_aggregate_patient_topic_source", "repartitioned_notification_topic_source")
                .addProcessor("patient_update_event_processor",
                        () -> new PatientUpdateEventProcessor("patient_store", "temp_notification_store"),
                        "type_tagging_processor")
                .addSink("patient_topic_sink",
                        patientAggregateTopicName,
                        Serdes.String().serializer(),
                        serializer,
                        "patient_update_event_processor");


        final StoreBuilder<KeyValueStore<String, Patient>> patientStoreBuilder =
                Stores.keyValueStoreBuilder(Stores
                        .inMemoryKeyValueStore("patient_store"), Serdes.String(), patientValueSerde)
                        .withLoggingDisabled();

        final StoreBuilder<KeyValueStore<String, DoctorAssignedToPatient>> tempNotificationStoreBuilder =
                Stores.keyValueStoreBuilder(Stores
                        .inMemoryKeyValueStore("temp_notification_store"), Serdes.String(), doctorAssignmentValueSerde)
                        .withLoggingDisabled();

        topology.addStateStore(patientStoreBuilder, "patient_update_event_processor")
                .addStateStore(tempNotificationStoreBuilder, "patient_update_event_processor");

        return topology;
    }


    public class TypeTaggingProcessor implements Processor<String, Serializable> {

        private ProcessorContext context;

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
        }

        @Override
        public void process(String key, Serializable value) {
            if (value instanceof Patient) {
                context.forward(key, new TypeWrapper<>(TypeWrapper.Type.PATIENT, value), To.all());
            } else if (value instanceof DoctorAssignedToPatient) {
                context.forward(key, new TypeWrapper<>(TypeWrapper.Type.DOCTOR_NOTIFICATION, value), To.all());
            } else {
                // Nothing
            }
        }

        @Override
        public void close() {
        }
    }

    public class PatientUpdateEventProcessor implements Processor<String, TypeWrapper> {

        private String patientStoreName;
        private String temporaryNotificationStoreName;
        private ProcessorContext context;
        private KeyValueStore<String, Patient> patientStore;
        private KeyValueStore<String, DoctorAssignedToPatient> temporaryNotificationStore;
        private long notificationValidityPeriod = 5000;

        public PatientUpdateEventProcessor(String patientStoreName, String temporaryNotificationStoreName) {
            this.patientStoreName = patientStoreName;
            this.temporaryNotificationStoreName = temporaryNotificationStoreName;
        }

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            this.patientStore = (KeyValueStore<String, Patient>) this.context.getStateStore(patientStoreName);
            this.temporaryNotificationStore =
                    (KeyValueStore<String, DoctorAssignedToPatient>) this.context.getStateStore(temporaryNotificationStoreName);
            this.context.schedule(Duration.ofMillis(4000), PunctuationType.WALL_CLOCK_TIME, new Punctuator() {
                @Override
                public void punctuate(long timestamp) {
                    long currentTime = System.currentTimeMillis();
                    List<String> expiredRecordKeys = new ArrayList<>();
                    try (KeyValueIterator<String, DoctorAssignedToPatient> iterator = temporaryNotificationStore.all()) {
                        iterator.forEachRemaining(
                                new Consumer<KeyValue<String, DoctorAssignedToPatient>>() {
                                    @Override
                                    public void accept(KeyValue<String, DoctorAssignedToPatient> record) {
                                        if (currentTime - record.value.getStoredTime() > notificationValidityPeriod) {
                                            expiredRecordKeys.add(record.key);
                                        }
                                    }
                                }
                        );

                    }
                    expiredRecordKeys.forEach(key -> temporaryNotificationStore.delete(key));
                }
            });
        }

        @Override
        public void process(String key, TypeWrapper value) {
            switch (value.getType()) {
                case PATIENT:
                    mayUpdateAndStoreOnPatientEvent((Patient) value.getValue());
                    break;
                case DOCTOR_NOTIFICATION:
                    mayUpdateOnNotification((DoctorAssignedToPatient) value.getValue());
                    break;
            }
        }

        private void mayUpdateOnNotification(DoctorAssignedToPatient value) {
            Patient patient = patientStore.get(value.getPatientId());
            if (patient != null && patient.getLastUpdatedTime().isBefore(value.getCreatedTime())) {
                patient.setDoctorId(value.getDoctorId());
                patient.setLastUpdatedTime(Instant.now());
                patientStore.put(patient.getId(), patient);
                context.forward(patient.getId(), patient, To.child("patient_event_sink"));
            }
            if (patient == null) {
                value.setStoredTime(System.currentTimeMillis());
                temporaryNotificationStore.put(value.getPatientId(), value);
            }
        }

        private void mayUpdateAndStoreOnPatientEvent(Patient patient) {
            DoctorAssignedToPatient updateNotification = temporaryNotificationStore.delete(patient.getId());
            if (updateNotification != null && updateNotification.getCreatedTime().isAfter(patient.getLastUpdatedTime())) {
                patient.setDoctorId(updateNotification.getDoctorId());
                patient.setLastUpdatedTime(Instant.now());
                context.forward(patient.getId(), patient, To.child("patient_event_sink"));
            }
            patientStore.put(patient.getId(), patient);
        }

        @Override
        public void close() {

        }
    }

    @Getter
    private static class TypeWrapper<T extends Serializable> {

        private enum Type {
            PATIENT,
            DOCTOR_NOTIFICATION,
        }

        TypeWrapper.Type type;

        T value;

        public TypeWrapper(TypeWrapper.Type type, T value) {
            this.type = type;
            this.value = value;
        }
    }
}
