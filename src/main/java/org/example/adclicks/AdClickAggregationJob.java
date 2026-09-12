package org.example.adclicks;

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

// Reads ad-click events from Kafka, counts clicks per ad in 1-minute tumbling
// windows (by event time), and upserts each window's result into Postgres.
public class AdClickAggregationJob {

    private static final String KAFKA_BOOTSTRAP = "kafka:29092"; // internal docker-network listener
    private static final String TOPIC = "ad-clicks";
    private static final String JDBC_URL = "jdbc:postgresql://postgres:5432/adsdb";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setTopics(TOPIC)
                .setGroupId("ad-click-aggregator")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new org.apache.flink.api.common.serialization.SimpleStringSchema())
                .build();

        DataStream<String> rawJson = env.fromSource(
                source, WatermarkStrategy.noWatermarks(), "kafka-ad-clicks");

        DataStream<AdClickEvent> events = rawJson.map(new JsonToEvent());

        DataStream<AdClickEvent> withWatermarks = events.assignTimestampsAndWatermarks(
                WatermarkStrategy.<AdClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, timestamp) -> event.ts));

        DataStream<AdClickWindowCount> counts = withWatermarks
                .keyBy(event -> event.adId)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .apply(new CountPerWindow());

        counts.addSink(JdbcSink.sink(
                "INSERT INTO ad_click_counts (ad_id, window_start, window_end, click_count) " +
                        "VALUES (?, ?, ?, ?) " +
                        "ON CONFLICT (ad_id, window_start) " +
                        "DO UPDATE SET window_end = EXCLUDED.window_end, click_count = EXCLUDED.click_count",
                (statement, result) -> {
                    statement.setString(1, result.adId);
                    statement.setTimestamp(2, new java.sql.Timestamp(result.windowStart));
                    statement.setTimestamp(3, new java.sql.Timestamp(result.windowEnd));
                    statement.setLong(4, result.count);
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

        env.execute("ad-click-window-aggregation");
    }

    private static class JsonToEvent implements MapFunction<String, AdClickEvent> {
        private transient ObjectMapper mapper;

        @Override
        public AdClickEvent map(String value) throws Exception {
            if (mapper == null) {
                mapper = new ObjectMapper();
            }
            return mapper.readValue(value, AdClickEvent.class);
        }
    }

    private static class CountPerWindow
            implements WindowFunction<AdClickEvent, AdClickWindowCount, String, TimeWindow> {
        @Override
        public void apply(String adId, TimeWindow window, Iterable<AdClickEvent> input,
                           Collector<AdClickWindowCount> out) {
            long count = 0;
            for (AdClickEvent ignored : input) {
                count++;
            }
            out.collect(new AdClickWindowCount(adId, window.getStart(), window.getEnd(), count));
        }
    }
}
