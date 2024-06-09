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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class StockDataProcessing {
    public static void main(String[] args) {
        if (args.length < 5) {
            System.err.println("Please provide the following parameters: <topic> <D (days)> <P (percentage)> <delay (A or C)> <bootstrap-servers>");
            System.exit(1);
        }

        String topic = args[0]; // temat Kafki
        int D = Integer.parseInt(args[1]); // długość okresu czasu w dniach
        double P = Double.parseDouble(args[2]) / 100; // minimalny stosunek różnicy kursów
        String delay = args[3]; // tryb przetwarzania: A lub C
        String bootstrapServers = args[4]; // Kafka bootstrap servers
        String staticDataPath = args[5];

        Map<String, List<StockData>> stockDataHistory = new HashMap<>(); // for anomaly detection
        System.out.println("Starting StockDataProcessing application with topic: " + topic + ", D: " + D + ", P: " + P + ", delay: " + delay);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stock-data-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        AggregationSerde aggregationSerde = new AggregationSerde();

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream(topic);
        source.peek((key, value) -> System.out.println("[START]: key=" + key + ", value=" + value));

        Map<String, String> symbolMap = StaticDataLoader.createHashMap(staticDataPath);
        System.out.println("Loaded static data: " + symbolMap.size() + " entries");

        KStream<String, StockData> stockData = source.mapValues(value -> {
            String[] fields = value.split(",");
            return new StockData(fields[0],
                    Double.parseDouble(fields[1]),
                    Double.parseDouble(fields[2]),
                    Double.parseDouble(fields[3]),
                    Double.parseDouble(fields[4]),
                    Double.parseDouble(fields[5]),
                    Double.parseDouble(fields[6]),
                    fields[7]);
        }).selectKey((oldKey, value) -> value.getStock());

        // Anomaly detection

        stockData.foreach((key, value) -> {
            String stockSymbol = value.getStock();
            List<StockData> history = stockDataHistory.computeIfAbsent(stockSymbol, k -> new ArrayList<>());

            // Add the current data to the history
            history.add(value);

            // Check for anomalies if history size exceeds D
            if (history.size() >= D) {
                // Find the start and end dates of the analyzed period
                LocalDate currentDate = value.getDate().toLocalDate();
                LocalDate startDate = null;

                // Find the start date by going back D-1 days from the current date
                LocalDate tempDate = currentDate.minusDays(D - 1);
                while (tempDate.isBefore(currentDate) || tempDate.isEqual(currentDate)) {
                    LocalDate finalTempDate = tempDate;
                    if (history.stream().anyMatch(data -> data.getDate().toLocalDate().isEqual(finalTempDate))) {
                        startDate = tempDate;
                        break;
                    }
                    tempDate = tempDate.plusDays(1); // Move to the next day
                }

                if (startDate == null) {
                    System.err.println("Error: Could not find start date for the analyzed period.");
                    return;
                }

                LocalDate endDate = currentDate; // End of the analyzed period

                // Filter the history list to include only the data points for the current stock symbol
                LocalDate finalStartDate = startDate;
                List<StockData> filteredHistory = history.stream()
                        .filter(data -> data.getStock().equals(stockSymbol))
                        .filter(data -> data.getDate().toLocalDate().isAfter(finalStartDate.minusDays(1)) &&
                                data.getDate().toLocalDate().isBefore(endDate.plusDays(1)))
                        .collect(Collectors.toList());

                double maxHigh = filteredHistory.stream().mapToDouble(StockData::getHigh).max().orElse(0);
                double minLow = filteredHistory.stream().mapToDouble(StockData::getLow).min().orElse(0);


                // Calculate the ratio difference between high and low prices
                double ratio = (maxHigh - minLow) / maxHigh;

                // If the ratio exceeds the threshold P, log the anomaly
                if (ratio > P) {
                    System.out.println("DETECTED ANOMALY: Stock Symbol: " + stockSymbol +
                            ", Analyzed Period: " + startDate + " to " + endDate +
                            ", Max High: " + maxHigh +
                            ", Min Low: " + minLow +
                            ", Ratio: " + ratio);
                    // Additional information can be logged here
                }
            }
        });

        // Aggregation
        AtomicLong id = new AtomicLong(0);
        TimeWindows timeWindows = TimeWindows.of(Duration.ofDays(D)).grace(Duration.ofDays(1));
        if (delay.equals("A")) {
            stockData
                    .map((key, value) -> {
                        // Extract year and month from the date
                        LocalDateTime date = value.getDate();
                        int year = date.getYear();
                        int month = date.getMonthValue();
                        // Create a new key with stock symbol, year, and month
                        String newKey = value.getStock() + "-" + year + "-" + month;
                        return new KeyValue<>(newKey, value);
                    })
                    .groupByKey(Grouped.with(Serdes.String(), new StockDataSerde()))
                    .windowedBy(TimeWindows.of(Duration.ofDays(30)).grace(Duration.ofDays(1)))
                    .aggregate(
                            Aggregation::new,
                            (aggKey, newValue, aggValue) -> aggValue.add(newValue),
                            Materialized.<String, Aggregation, WindowStore<Bytes, byte[]>>as("aggregated-stream-store")
                                    .withKeySerde(Serdes.String())
                                    .withValueSerde(new AggregationSerde())
                    )
                    .toStream()
                    .peek((windowedKey, value) -> {
                        // Extract year and month from the windowed key
                        String key = windowedKey.key();
                        String[] parts = key.split("-");
                        String stockSymbol = parts[0];
                        int year = Integer.parseInt(parts[1]);
                        int month = Integer.parseInt(parts[2]);
                        System.out.println(value.toJsonString(stockSymbol, symbolMap.getOrDefault(stockSymbol, ""), year, month, id.incrementAndGet()));
                    })
                    .mapValues((windowedKey, value) -> {
                        // Extract year and month from the windowed key
                        String key = windowedKey.key();
                        String[] parts = key.split("-");
                        String stockSymbol = parts[0];
                        int year = Integer.parseInt(parts[1]);
                        int month = Integer.parseInt(parts[2]);

                        // Create the JSON string
                        return value.toJsonString(stockSymbol, symbolMap.getOrDefault(stockSymbol, ""), year, month, id.get());
                    })
                    .to("aggregated-stock-data", Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class), Serdes.String()));
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
