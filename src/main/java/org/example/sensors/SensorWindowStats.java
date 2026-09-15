package org.example.sensors;

import java.sql.Timestamp;

public class SensorWindowStats {
    public final String sensorId;
    public final long windowStart;
    public final long windowEnd;
    public final double minTemp;
    public final double maxTemp;
    public final double avgTemp;
    public final long readingCount;

    public SensorWindowStats(
            String sensorId,
            long windowStart,
            long windowEnd,
            double minTemp,
            double maxTemp,
            double avgTemp,
            long reading_count
        ) {
            this.sensorId = sensorId;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
            this.minTemp = minTemp;
            this.maxTemp = maxTemp;
            this.avgTemp = avgTemp;
            this.readingCount = reading_count;
    }

    @Override
    public String toString() {
        return sensorId + " [" + new Timestamp(windowStart) + " - " + new Timestamp(windowEnd) + "] -> " + readingCount;
    }
}
