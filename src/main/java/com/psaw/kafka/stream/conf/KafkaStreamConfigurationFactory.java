package com.psaw.kafka.stream.conf;

import java.util.Properties;

/**
 * <p>
 * <code>{@link KafkaStreamConfigurationFactory}</code> -
 * Factory class for Kafka Stream Configurations.
 * </p>
 */
public class KafkaStreamConfigurationFactory {

    private final Properties templateConfiguration;

    public KafkaStreamConfigurationFactory(Properties templateConfiguration) {
        this.templateConfiguration = templateConfiguration;
    }

    public Builder builder() {
        return new Builder((Properties) templateConfiguration.clone());
    }

    public static class Builder {
        private Properties properties;

        public Builder(Properties properties) {
            this.properties = properties;
        }

        public Builder configuration(String key, Object value) {
            properties.put(key, value);
            return this;
        }

        public Properties build() {
            return properties;
        }
    }
}
