package com.example.bigdata;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class StockDataSerde implements Serde<StockData> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public void close() {
    }

    @Override
    public Serializer<StockData> serializer() {
        return new Serializer<StockData>() {
            @Override
            public byte[] serialize(String topic, StockData data) {
                return data.toString().getBytes();
            }
        };
    }

    @Override
    public Deserializer<StockData> deserializer() {
        return new Deserializer<StockData>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
            }

            @Override
            public StockData deserialize(String topic, byte[] data) {
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
                return new StockData(date, open, high, low, close, adjClose, volume, stock);
            }

            @Override
            public void close() {
            }
        };
    }
}
