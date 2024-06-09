package com.example.bigdata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Aggregation {
    private double sumClose;
    private double minLow;
    private double maxHigh;
    private double sumVolume;
    private int count;
    private double avgClose;

    public Aggregation() {
        this.sumClose = 0.0;
        this.minLow = Double.MAX_VALUE;
        this.maxHigh = Double.MIN_VALUE;
        this.sumVolume = 0;
        this.count = 0;
        this.avgClose = 0.0;
    }

    public Aggregation add(StockData data) {
        this.sumClose += data.getClose();
        this.minLow = Math.min(this.minLow, data.getLow());
        this.maxHigh = Math.max(this.maxHigh, data.getHigh());
        this.sumVolume += data.getVolume();
        this.count++;
        this.avgClose = this.sumClose / this.count;
        return this;
    }

    public boolean isAnomaly(double threshold) {
        double ratio = (this.maxHigh - this.minLow) / this.maxHigh;
        return ratio > threshold;
    }

    public double getSumClose() {
        return sumClose;
    }
    public double getMinLow() {
        return minLow;
    }
    public double getMaxHigh() {
        return maxHigh;
    }
    public double getSumVolume() {
        return sumVolume;
    }
    public int getCount() {
        return count;
    }
    public double getAvgClose() {
        return avgClose;
    }



    public String toJsonString(String stockSymbol, String stockName, int year, int month, long id) {
        ObjectMapper objectMapper = new ObjectMapper();

        try {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("stockSymbol", stockSymbol);
            payload.put("stockName", stockName);
            payload.put("year", year);
            payload.put("month", month);
            payload.put("avgClose", getAvgClose());
            payload.put("minLow", getMinLow());
            payload.put("maxHigh", getMaxHigh());
            payload.put("sumVolume", sumVolume);

            Map<String, Object> schema = new LinkedHashMap<>();
            schema.put("type", "struct");
            schema.put("optional", false);
            schema.put("version", 1);
            List<Map<String, Object>> fields = new ArrayList<>();
            fields.add(Map.of("field", "stockSymbol", "type", "string", "optional", true));
            fields.add(Map.of("field", "stockName", "type", "string", "optional", true));
            fields.add(Map.of("field", "year", "type", "int32", "optional", true));
            fields.add(Map.of("field", "month", "type", "int32", "optional", true));
            fields.add(Map.of("field", "avgClose", "type", "float", "optional", true));
            fields.add(Map.of("field", "minLow", "type", "float", "optional", true));
            fields.add(Map.of("field", "maxHigh", "type", "float", "optional", true));
            fields.add(Map.of("field", "sumVolume", "type", "int64", "optional", true));
            schema.put("fields", fields);

            Map<String, Object> logEntry = new LinkedHashMap<>();
            logEntry.put("schema", schema);
            logEntry.put("payload", payload);

            // Use the ObjectMapper to convert the map to a JSON string
            String jsonLogEntry = objectMapper.writeValueAsString(logEntry);

            // Concatenate id with the JSON string
            return jsonLogEntry;
        } catch (Exception e) {
            throw new RuntimeException("JSON serialization failed", e);
        }
    }



}

