package com.psaw.kafka.stream.util.serde;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonPOJODeserializer<T> implements Deserializer<T> {

    private static final Logger logger = LoggerFactory.getLogger(JsonPOJODeserializer.class);

    private ObjectMapper objectMapper;

    private TypeReference<T> typeReference;

    private Class<T> classType;

    public JsonPOJODeserializer(TypeReference<T> typeReference) {
        this();
        this.typeReference = typeReference;
    }

    public JsonPOJODeserializer(Class<T> classType) {
        this();
        this.classType = classType;
    }

    private JsonPOJODeserializer() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.findAndRegisterModules()
                .registerModule(new JavaTimeModule())
                .registerModule(new Jdk8Module())
                .enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT)
                .enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Override
    public T deserialize(String topic, Headers headers, byte[] data) {
        return deserialize(topic, data);
    }

    @Override
    public T deserialize(String topic, byte[] bytes) {
        if (bytes == null)
            return null;
        if (typeReference == null && classType == null) {
            throw new SerializationException("Type information is not provided");
        }
        try {
            if (classType != null) {
                return objectMapper.readValue(bytes, classType);
            }
            return objectMapper.readValue(bytes, typeReference);
        } catch (Exception e) {
            String typeName = typeReference == null ? null : typeReference.getType().getTypeName();
            logger.warn("Error occurred while de-serializing. Type reference [{}]", typeName);
            throw new SerializationException("Error de=serializing JSON message", e);
        }
    }

    @Override
    public void close() {}
}
