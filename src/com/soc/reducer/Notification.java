package com.soc.reducer;

/**
 * Immutable record representing an incoming raw notification event ingested from distributed sources.
 * 
 * @param id               Unique system identifier for the raw notification
 * @param source           Originating service, host, or gateway (e.g., "auth-service-01", "whatsapp-gw-west")
 * @param routingKey       Hierarchical topic routing key (e.g., "sec.auth.bruteforce", "comm.whatsapp.group")
 * @param title            Short human-readable title or event description
 * @param payload          Raw notification body or JSON snippet
 * @param timestamp        Epoch timestamp in milliseconds when the event was generated
 * @param deduplicationKey Unique fingerprint used to identify identical or duplicate event streams in O(1)
 */
public record Notification(
        String id,
        String source,
        String routingKey,
        String title,
        String payload,
        long timestamp,
        String deduplicationKey
) {
    /**
     * Constructs a Notification with an automatically synthesized deduplication key
     * derived from source, routingKey, and title.
     * 
     * Time Complexity: O(L) where L is the combined string length.
     * Space Complexity: O(L) to allocate the combined deduplication key string.
     */
    public static Notification of(String id, String source, String routingKey, String title, String payload, long timestamp) {
        String dedupKey = source + "|" + routingKey + "|" + title.hashCode();
        return new Notification(id, source, routingKey, title, payload, timestamp, dedupKey);
    }
}
