package org.example.adclicks;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.Random;

// Simulates ad-click traffic. Run this from the host (uses localhost:9092),
// separate from AdClickAggregationJob, which runs inside the Flink cluster.
public class AdClickProducer {

    private static final String TOPIC = "ad-clicks";
    private static final List<String> AD_IDS = List.of("ad-1", "ad-2", "ad-3", "ad-4", "ad-5");

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        ObjectMapper mapper = new ObjectMapper();
        Random random = new Random();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            while (true) {
                String adId = AD_IDS.get(random.nextInt(AD_IDS.size()));
                String userId = "user-" + random.nextInt(1000);
                AdClickEvent event = new AdClickEvent(adId, userId, System.currentTimeMillis());

                String json = mapper.writeValueAsString(event);
                producer.send(new ProducerRecord<>(TOPIC, adId, json), (metadata, exception) -> {
                    if (exception != null) {
                        exception.printStackTrace();
                    }
                });

                System.out.println("sent: " + json);
                Thread.sleep(100);
            }
        }
    }
}
