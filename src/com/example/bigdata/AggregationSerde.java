package com.example.bigdata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class AggregationSerde implements Serde<Aggregation> {
    private ObjectMapper objectMapper = new ObjectMapper();

    public AggregationSerde() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public void close() {
    }

    @Override
    public Serializer<Aggregation> serializer() {
        return new Serializer<Aggregation>() {
            @Override
            public byte[] serialize(String topic, Aggregation data) {
                try {
                    return objectMapper.writeValueAsBytes(data);
                } catch (Exception e) {
                    throw new RuntimeException("Serialization failed", e);
                }
            }
        };
    }

    @Override
    public Deserializer<Aggregation> deserializer() {
        return new Deserializer<Aggregation>() {
            @Override
            public Aggregation deserialize(String topic, byte[] data) {
                try {
                    return objectMapper.readValue(data, Aggregation.class);
                } catch (Exception e) {
                    throw new RuntimeException("Deserialization failed", e);
                }
            }
        };
    }
}