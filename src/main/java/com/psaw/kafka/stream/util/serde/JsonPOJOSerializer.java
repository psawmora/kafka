package com.psaw.kafka.stream.util.serde;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonPOJOSerializer<T> implements Serializer<T> {

    private final ObjectMapper objectMapper;

    private final Logger logger = LoggerFactory.getLogger(JsonPOJOSerializer.class);

    public JsonPOJOSerializer() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.findAndRegisterModules()
                .registerModule(new JavaTimeModule())
                .registerModule(new Jdk8Module())
                .enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT)
                .enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY);
    }

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null)
            return null;
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            logger.warn("Error occurred while serializing the record [{}]", data);
            throw new SerializationException("Error serializing JSON message", e);
        }
    }

    @Override
    public void close() {
    }
}
