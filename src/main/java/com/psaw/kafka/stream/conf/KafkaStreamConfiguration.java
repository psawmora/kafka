package com.psaw.kafka.stream.conf;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DefaultProductionExceptionHandler;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import java.util.Properties;

import static org.apache.kafka.streams.StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG;

/**
 * <p>
 * <code>{@link KafkaStreamConfiguration}</code> -
 * Contains configurations for Kafka Stream Apps.
 * </p>
 */
@Configuration
public class KafkaStreamConfiguration {

    @Bean
    @Qualifier("application-configuration-factory")
    public KafkaStreamConfigurationFactory streamAppConfigurationFactory(
            @Value("${bootstrap.brokers:127.0.0.1:9092}") String bootstrapServers) {
        Properties appConfiguration = new Properties();
        appConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        appConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        appConfiguration.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        appConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        appConfiguration.put(DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, DefaultProductionExceptionHandler.class);
        appConfiguration.put(DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        return new KafkaStreamConfigurationFactory(appConfiguration);
    }

    // Important - This is for creating internal topics.
    @Bean
    @Qualifier("internal-topic-configuration-factory")
    public KafkaStreamConfigurationFactory interalTopicConfigurationFactory(
            @Value("${kafka.internal.topic.compact.configuration:delete}") String compactConfiguration,
            @Value("${kafka.internal.topic.min.in.sync.replica:1}") String minimumInSyncReplicaCount) {
        Properties properties = new Properties();
        properties.setProperty(TopicConfig.CLEANUP_POLICY_COMPACT, compactConfiguration);
        properties.setProperty(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minimumInSyncReplicaCount);
        return new KafkaStreamConfigurationFactory(properties);
    }
}
