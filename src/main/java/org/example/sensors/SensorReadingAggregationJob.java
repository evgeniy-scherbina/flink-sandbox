package org.example.sensors;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class SensorReadingAggregationJob {
    private static final String KAFKA_BOOTSTRAP = "kafka:29092"; // internal docker-network listener
    private static final String TOPIC = "sensor-readings";
    private static final String JDBC_URL = "jdbc:postgresql://postgres:5432/adsdb";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setTopics(TOPIC)
                .setGroupId("sensor-readings-aggregator")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new org.apache.flink.api.common.serialization.SimpleStringSchema())
                .build();

        DataStream<String> rawJson = env.fromSource(
                source, WatermarkStrategy.noWatermarks(), "kafka-ensor-readings");

        DataStream<SensorReading> events = rawJson.map(new SensorReadingAggregationJob.JsonToEvent());

        DataStream<SensorReading> withWatermarks = events.assignTimestampsAndWatermarks(
                WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, timestamp) -> event.ts));

        DataStream<SensorWindowStats> counts = withWatermarks
                .keyBy(event -> event.sensorId)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .apply(new SensorReadingAggregationJob.StatsPerWindow());

        counts.addSink(JdbcSink.sink(
                "INSERT INTO sensor_window_stats (sensor_id, window_start, window_end, min_temp, max_temp, avg_temp, reading_count) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?) " +
                        "ON CONFLICT (sensor_id, window_start) " +
                        "DO UPDATE SET" +
                        "window_end = EXCLUDED.window_end," +
                        "min_temp = EXCLUDED.min_temp," +
                        "max_temp = EXCLUDED.max_temp," +
                        "avg_temp = EXCLUDED.avg_temp," +
                        "reading_count = EXCLUDED.reading_count",
                (statement, result) -> {
                    statement.setString(1, result.sensorId);
                    statement.setTimestamp(2, new java.sql.Timestamp(result.windowStart));
                    statement.setTimestamp(3, new java.sql.Timestamp(result.windowEnd));
                    statement.setDouble(4, result.minTemp);
                    statement.setDouble(5, result.maxTemp);
                    statement.setDouble(6, result.avgTemp);
                    statement.setLong(7, result.readingCount);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(JDBC_URL)
                        .withDriverName("org.postgresql.Driver")
                        .withUsername("flink")
                        .withPassword("flink")
                        .build()));
    }

    private static class JsonToEvent implements MapFunction<String, SensorReading> {
        private transient ObjectMapper mapper;

        @Override
        public SensorReading map(String value) throws Exception {
            if (mapper == null) {
                mapper = new ObjectMapper();
            }
            return mapper.readValue(value, SensorReading.class);
        }
    }

    private static class StatsPerWindow
            implements WindowFunction<SensorReading, SensorWindowStats, String, TimeWindow> {

        @Override
        public void apply(String adId, TimeWindow window, Iterable<SensorReading> input,
                          Collector<SensorWindowStats> out) {
            double minTemp = 0;
            double maxTemp = 0;
            double avgTemp = 0;
            long readingCount = 0;
            for (SensorReading ignored : input) {
                // TODO: add min/max/avg
                readingCount++;
            }

            out.collect(new SensorWindowStats(adId, window.getStart(), window.getEnd(), minTemp, maxTemp, avgTemp, readingCount));
        }
    }
}
