package com.psaw.kafka.stream.util;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * <p>
 * <code>{@link TopicAndStoreUtil}</code> -
 * Topic creator facilitates the creation of topics for Kafka Streaming Apps.
 * </p>
 */
public class TopicAndStoreUtil {

    private static final Logger logger = LoggerFactory.getLogger(TopicAndStoreUtil.class);

    public static <V extends Serializable> Materialized<String, V, KeyValueStore<Bytes, byte[]>> getStateStoreMaterialized(
            String storeName,
            Serde<V> valueSerde) {
        return Materialized
                .<String, V>as(Stores.inMemoryKeyValueStore(storeName))
                .withKeySerde(Serdes.String()).withValueSerde(valueSerde);
    }

    public static void createTopics(List<String> topics,
                                    int partitionCount,
                                    short replicationCount,
                                    Properties applicationConfiguration) {
        logger.info("Creating topics [{}] for the application configuration [{}]", topics, applicationConfiguration);
        AdminClient admin = AdminClient.create(applicationConfiguration);
        List<NewTopic> newTopics =
                topics.stream().filter(s -> !s.trim().isEmpty())
                        .map(topic -> new NewTopic(topic.trim(), partitionCount, replicationCount)).collect(Collectors.toList());
        createAndWaitForTopicsUsingAdmin(admin, topics, newTopics);
    }

    public static void createTopic(String topic,
                                   int partitionCount,
                                   short replicationCount,
                                   Properties applicationConfiguration) {
        AdminClient admin = AdminClient.create(applicationConfiguration);
        List<String> topics = new ArrayList<>();
        topics.add(topic);
        List<NewTopic> newTopics = new ArrayList<>();
        newTopics.add(new NewTopic(topic.trim(), partitionCount, replicationCount));
        createAndWaitForTopicsUsingAdmin(admin, topics, newTopics);
    }

    private static void createAndWaitForTopicsUsingAdmin(AdminClient admin, List<String> topics, List<NewTopic> newTopics) {
        while (true) {
            logger.info("Waiting for created topics to be propagated....");
            try {
                if (admin.listTopics().names().get(5000, TimeUnit.MILLISECONDS).containsAll(topics)) break;
                admin.createTopics(newTopics);
            } catch (Throwable th) {
                logger.error("Error occurred while creating topics and retrying...", th);
            }
        }
        logger.info("Finished topic creation [Topics - {}]", topics);
        try {
            DescribeTopicsResult describeTopicsResult = admin.describeTopics(topics);
            describeTopicsResult.all().get().forEach(new BiConsumer<String, TopicDescription>() {
                @Override
                public void accept(String s, TopicDescription topicDescription) {
                    logger.info("Topic [{}] | Partition [{}]", topicDescription.name(), topicDescription.partitions());
                }
            });
        } catch (Exception e) {
        }
    }
}
