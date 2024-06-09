package com.example.bigdata;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class StockSerde implements Serde<LogRecords> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public void close() {
    }

    @Override
    public Serializer<LogRecords> serializer() {
        return new Serializer<LogRecords>() {
            @Override
            public byte[] serialize(String topic, LogRecords data) {
                return data.toString().getBytes();
            }
        };
    }

    @Override
    public Deserializer<LogRecords> deserializer() {
        return new Deserializer<LogRecords>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
            }

            @Override
            public LogRecords deserialize(String topic, byte[] data) {
                String value = new String(data);
                String[] parts = value.split(",");
                String date = null;
                double open = 0.0;
                double high = 0.0;
                double low = 0.0;
                double close = 0.0;
                double adjClose = 0.0;
                double volume = 0.0;
                String stock = null;

                date = parts[0].split("=")[1].trim();
                open = Double.parseDouble(parts[1].split("=")[1].trim());
                high = Double.parseDouble(parts[2].split("=")[1].trim());
                low = Double.parseDouble(parts[3].split("=")[1].trim());
                close = Double.parseDouble(parts[4].split("=")[1].trim());
                adjClose = Double.parseDouble(parts[5].split("=")[1].trim());
                volume = Double.parseDouble(parts[6].split("=")[1].trim());
                stock = parts[7].split("=")[1].trim();
                return new LogRecords(date, open, high, low, close, adjClose, volume, stock);
            }

            @Override
            public void close() {
            }
        };
    }
}
