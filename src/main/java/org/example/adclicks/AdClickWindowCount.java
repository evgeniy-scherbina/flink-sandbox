package org.example.adclicks;

import java.sql.Timestamp;

public class AdClickWindowCount {
    public final String adId;
    public final long windowStart;
    public final long windowEnd;
    public final long count;

    public AdClickWindowCount(String adId, long windowStart, long windowEnd, long count) {
        this.adId = adId;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    @Override
    public String toString() {
        return adId + " [" + new Timestamp(windowStart) + " - " + new Timestamp(windowEnd) + "] -> " + count;
    }
}
