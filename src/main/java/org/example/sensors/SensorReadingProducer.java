package org.example.sensors;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.Random;

public class SensorReadingProducer {
    private static final String TOPIC = "sensor-readings";
    private static final List<String> SENSOR_IDS = List.of("sensor-1", "sensor-2", "sensor-3", "sensor-4", "sensor-5");

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        ObjectMapper mapper = new ObjectMapper();
        Random random = new Random();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            while (true) {
                String sensorId = SENSOR_IDS.get(random.nextInt(SENSOR_IDS.size()));
                double temperature = random.nextDouble() * 30;
                SensorReading reading = new SensorReading(sensorId, temperature, System.currentTimeMillis());

                String json = mapper.writeValueAsString(reading);
                producer.send(new ProducerRecord<>(TOPIC, sensorId, json), (metadata, exception) -> {
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
