package org.example.adclicks;

public class AdClickEvent {

    public String adId;
    public String userId;
    public long ts;

    // Needed by Jackson for deserialization.
    public AdClickEvent() {
    }

    public AdClickEvent(String adId, String userId, long ts) {
        this.adId = adId;
        this.userId = userId;
        this.ts = ts;
    }
}
