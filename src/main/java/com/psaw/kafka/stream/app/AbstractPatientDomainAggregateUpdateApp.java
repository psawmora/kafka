package com.psaw.kafka.stream.app;

import com.psaw.kafka.stream.conf.KafkaStreamConfigurationFactory;
import com.psaw.kafka.stream.domain.entity.Patient;
import com.psaw.kafka.stream.domain.event.DoctorAssignedToPatient;
import com.psaw.kafka.stream.domain.event.Event;
import com.psaw.kafka.stream.util.TopicAndStoreUtil;
import com.psaw.kafka.stream.util.serde.JsonPOJODeserializer;
import com.psaw.kafka.stream.util.serde.JsonPOJOSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.apache.kafka.common.serialization.Serdes.serdeFrom;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.NUM_STREAM_THREADS_CONFIG;

/**
 * <p>
 * <code>{@link AbstractPatientDomainAggregateUpdateApp}</code> -
 * Contains common attributes and behaviors.
 * </p>
 */
public abstract class AbstractPatientDomainAggregateUpdateApp {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Value("${kafka.bootstrap.servers:127.0.0.1:9092}")
    private String bootstrapServers;

    protected String notificationTopicName = "notification_domain_entity_doctor";

    protected String patientAggregateTopicName = "domain_aggregate_patient";

    protected String appName = "patient-aggregate-stream-app";

    protected final KafkaStreamConfigurationFactory configurationFactory;

    protected KafkaStreams streams;

    protected Properties appConfiguration;

    public AbstractPatientDomainAggregateUpdateApp(KafkaStreamConfigurationFactory configurationFactory) {
        this.configurationFactory = configurationFactory;
    }

    @PreDestroy
    public void cleanup() {
        logger.info("Stopping the stream application - [{}]", appName);
        streams.close();
    }

    protected JsonPOJOSerializer serializer = new JsonPOJOSerializer<>();
    protected JsonPOJODeserializer<Event> eventDeserializer = new JsonPOJODeserializer<>(Event.class);
    protected Serde<Event> eventSerde = serdeFrom(serializer, eventDeserializer);
    protected JsonPOJODeserializer<Patient> patientDeserializer = new JsonPOJODeserializer<>(Patient.class);
    protected Serde<Patient> patientValueSerde = serdeFrom(serializer, patientDeserializer);
    protected JsonPOJODeserializer<DoctorAssignedToPatient> doctorAssignmentDeserializer = new JsonPOJODeserializer<>(DoctorAssignedToPatient.class);
    protected Serde<DoctorAssignedToPatient> doctorAssignmentValueSerde = serdeFrom(serializer, doctorAssignmentDeserializer);

    @PostConstruct
    public void init() {
        logger.info("Initializing the stream application [{}]", appName);
        this.appConfiguration = configurationFactory.builder()
                .configuration(APPLICATION_ID_CONFIG, appName)
                .configuration(NUM_STREAM_THREADS_CONFIG, 10)
                .build();
        List<String> externalTopics = new ArrayList<>();
        try {
            TopicAndStoreUtil.createTopic(notificationTopicName, 5, (short)1, appConfiguration);
            TopicAndStoreUtil.createTopic(patientAggregateTopicName, 4, (short)1, appConfiguration);
            Topology topology = buildStream(externalTopics);
            logger.info("{} Topology ------------ \n\n", appName);
            logger.info(topology.describe().toString());
            logger.info("------------\n\n\n\n");
            this.streams = new KafkaStreams(topology, appConfiguration);
            this.streams.start();
        } catch (Throwable e) {
            logger.error("Error occurred while starting the Stream Application [{}] - [{}]", appName, e);
            throw e;
        }
    }

    protected abstract Topology buildStream(List<String> externalTopics);

}
