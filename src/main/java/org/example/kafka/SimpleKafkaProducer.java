package org.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SimpleKafkaProducer {

    private static final String TOPIC = "demo-topic";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 1; i <= 5; i++) {
                String key = "key-" + i;
                String value = "hello kafka " + i;

                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, value);

                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        exception.printStackTrace();
                    } else {
                        System.out.printf("sent key=%s value=%s -> partition=%d offset=%d%n",
                                key, value, metadata.partition(), metadata.offset());
                    }
                });
            }
            producer.flush();
        }
    }
}
