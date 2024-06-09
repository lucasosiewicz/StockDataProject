package com.example.bigdata;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class StockApp {
    public static void main(String[] args) {

        int D = Integer.parseInt(args[1]);
        double P = Double.parseDouble(args[2]) / 100;
        String delay = args[3];
        String kafkaTopic = args[0];
        String clusterAddress = args[4];
        String staticDataPath = args[5];

        Map<String, List<LogRecords>> hashMapPeriod = new HashMap<>(); // for anomaly detection
        System.out.println("Starting StockDataProcessing application with topic: " + kafkaTopic + ", D: " + D + ", P: " + P + ", delay: " + delay);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "second-project");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, clusterAddress);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream(kafkaTopic);
        source.peek((key, value) -> System.out.println("[START]: key=" + key + ", value=" + value));

        Map<String, String> dictionary = StaticDataLoader.createHashMap(staticDataPath);
        System.out.println("Loaded static data: " + dictionary.size() + " entries");

        KStream<String, LogRecords> splittedData = source.mapValues(value -> {
            String[] fields = value.split(",");
            return new LogRecords(fields[0],
                    Double.parseDouble(fields[1]),
                    Double.parseDouble(fields[2]),
                    Double.parseDouble(fields[3]),
                    Double.parseDouble(fields[4]),
                    Double.parseDouble(fields[5]),
                    Double.parseDouble(fields[6]),
                    fields[7]);
        }).selectKey((key, value) -> value.getStock());

        // Anomaly detection

        splittedData.foreach((key, value) -> {
            String name = value.getStock();
            List<LogRecords> period = hashMapPeriod.computeIfAbsent(name, k -> new ArrayList<>());

            // Add the current data to the history
            period.add(value);

            // Check for anomalies if history size exceeds D
            if (period.size() >= D) {
                // Find the start and end dates of the analyzed period
                LocalDate start = null;
                LocalDate now = value.getDate().toLocalDate();

                // Find the start date by going back D-1 days from the current date
                LocalDate tempDate = now.minusDays(D - 1);
                while (tempDate.isBefore(now) || tempDate.isEqual(now)) {
                    LocalDate finalTempDate = tempDate;
                    if (period.stream().anyMatch(data -> data.getDate().toLocalDate().isEqual(finalTempDate))) {
                        start = tempDate;
                        break;
                    }
                    tempDate = tempDate.plusDays(1); // Move to the next day
                }

                LocalDate end = now; // End of the analyzed period

                // Filter the history list to include only the data points for the current stock symbol
                LocalDate finalStartDate = start;
                List<LogRecords> batchedPeriod = period.stream()
                        .filter(data -> data.getStock().equals(name))
                        .filter(data -> data.getDate().toLocalDate().isAfter(finalStartDate.minusDays(1)) &&
                                data.getDate().toLocalDate().isBefore(end.plusDays(1)))
                        .collect(Collectors.toList());

                double maxStock = batchedPeriod.stream().mapToDouble(LogRecords::getHigh).max().orElse(0);
                double minStock = batchedPeriod.stream().mapToDouble(LogRecords::getLow).min().orElse(0);


                // Calculate the ratio difference between high and low prices
                double diff = (maxStock - minStock) / maxStock;

                // If the ratio exceeds the threshold P, log the anomaly
                if (diff > P) {
                    System.out.println("DETECTED ANOMALY: Dates: " + start + " :: " + end +
                            ", Stock Name: " + name +
                            ", Highest Stock Price: " + maxStock +
                            ", Lowest Stock Price: " + minStock +
                            ", Difference: " + diff);
                }
            }
        });

        // Aggregation
        AtomicLong id = new AtomicLong(0);
        TimeWindows timeWindows = TimeWindows.of(Duration.ofDays(D)).grace(Duration.ofDays(1));
        if (delay.equals("A")) {
            splittedData
                    .map((key, value) -> {
                        // Extract year and month from the date
                        LocalDateTime date = value.getDate();
                        int year = date.getYear();
                        int month = date.getMonthValue();
                        // Create a new key with stock symbol, year, and month
                        String newKey = value.getStock() + "." + year + "." + month;
                        return new KeyValue<>(newKey, value);
                    })
                    .groupByKey(Grouped.with(Serdes.String(), new StockSerde()))
                    .windowedBy(TimeWindows.of(Duration.ofDays(30)).grace(Duration.ofDays(1)))
                    .aggregate(
                            KafkaAggregator::new,
                            (aggKey, newValue, aggValue) -> aggValue.add(newValue),
                            Materialized.<String, KafkaAggregator, WindowStore<Bytes, byte[]>>as("storage")
                                    .withKeySerde(Serdes.String())
                                    .withValueSerde(new AggregationSerde())
                    )
                    .toStream()
                    .peek((windowedKey, value) -> {
                        // Extract year and month from the windowed key
                        String key = windowedKey.key();
                        String[] parts = key.split("\\.");
                        String stockSymbol = parts[0];
                        int year = Integer.parseInt(parts[1]);
                        int month = Integer.parseInt(parts[2]);
                        System.out.println(value.toJsonString(stockSymbol, dictionary.getOrDefault(stockSymbol, ""), year, month, id.incrementAndGet()));
                    })
                    .mapValues((windowedKey, value) -> {
                        // Extract year and month from the windowed key
                        String key = windowedKey.key();
                        String[] parts = key.split("-");
                        String stockSymbol = parts[0];
                        int year = Integer.parseInt(parts[1]);
                        int month = Integer.parseInt(parts[2]);

                        // Create the JSON string
                        return value.toJsonString(stockSymbol, dictionary.getOrDefault(stockSymbol, ""), year, month, id.get());
                    })
                    .to("second", Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class), Serdes.String()));
        }


        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        streams.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
            System.err.println("Uncaught exception in thread " + thread.getName() + ": " + throwable.getMessage());
            throwable.printStackTrace();
        });

        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
