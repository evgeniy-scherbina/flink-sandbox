package org.example.sensors;

public class SensorReading {

    public String sensorId;
    public double temperature;
    public long ts;

    // Needed by Jackson for deserialization.
    public SensorReading() {
    }

    public SensorReading(String sensorId, double temperature, long ts) {
        this.sensorId = sensorId;
        this.temperature = temperature;
        this.ts = ts;
    }
}
