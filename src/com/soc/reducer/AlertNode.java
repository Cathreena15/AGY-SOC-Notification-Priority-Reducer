package com.soc.reducer;

/**
 * AlertNode: The foundational node entity representing a consolidated, deduplicated alert.
 * 
 * <p>Architectural Synergy: This class serves as the core node in both:
 * <ol>
 *   <li>The <b>O1Reducer</b> Doubly Linked List (via {@code prev} and {@code next} pointers)</li>
 *   <li>The <b>PriorityMaxHeap</b> priority matrix (indexed via binary heap array and hash map)</li>
 * </ol>
 * </p>
 * 
 * <p>Dynamic Priority Calculation:
 * Raw base weight is assigned via {@link TopicTrie}. When duplicate events stream in,
 * the priority score scales algorithmically according to a logarithmic frequency curve:
 * <pre>
 *   priority = Math.min(100.0, baseWeight + (Math.log(frequency) / Math.log(2)) * 3.5);
 * </pre>
 * This prevents low-priority spam from instantly overwhelming critical P0 infrastructure alerts,
 * while still elevating high-frequency denial-of-service or barrage patterns.
 * </p>
 */
public class AlertNode {

    private final String id;
    private final String deduplicationKey;
    private final String source;
    private final String routingKey;
    private final String title;
    private String payload;
    private final String category;
    private String severity;
    private final double baseWeight;

    // Mutated dynamically upon deduplication
    private volatile int frequency;
    private final long firstSeenMs;
    private volatile long lastSeenMs;
    private volatile double dynamicPriority;
    private volatile boolean justUpdated;

    // Pointers for Doubly Linked List in O1Reducer
    AlertNode prev;
    AlertNode next;

    /**
     * Constructs a new AlertNode from an initial raw notification and matched rule metadata.
     * 
     * Time Complexity: O(1)
     * Space Complexity: O(1)
     */
    public AlertNode(Notification notification, RuleMetadata rule) {
        this.id = notification.id();
        this.deduplicationKey = notification.deduplicationKey();
        this.source = notification.source();
        this.routingKey = notification.routingKey();
        this.title = notification.title();
        this.payload = notification.payload();
        this.category = rule.category();
        this.severity = rule.severity();
        this.baseWeight = rule.baseWeight();

        this.frequency = 1;
        this.firstSeenMs = notification.timestamp();
        this.lastSeenMs = notification.timestamp();
        this.dynamicPriority = rule.baseWeight();
        this.justUpdated = false;

        this.prev = null;
        this.next = null;
    }

    /**
     * Increments frequency counter, updates timestamp and payload,
     * and recalibrates the dynamic priority score.
     * 
     * Time Complexity: O(1)
     * Space Complexity: O(1)
     */
    public void recordDuplicateOccurrence(Notification notification) {
        this.frequency++;
        this.lastSeenMs = notification.timestamp();
        this.payload = notification.payload();
        this.justUpdated = true;

        // Dynamic formula: Base weight + logarithmic scaling of frequency boost
        // A low-priority spam notification (base 15.0) with 1,000 duplicates reaches:
        // 15.0 + 9.96 * 3.5 = 49.86 (P2 tier), ensuring it never eclipses genuine P0 alerts (95+).
        double boost = (Math.log(this.frequency) / Math.log(2.0)) * 3.5;
        this.dynamicPriority = Math.min(100.0, this.baseWeight + boost);

        // Adjust severity tier dynamically if score crossed critical threshold
        if (this.dynamicPriority >= 90.0) {
            this.severity = "P0_CRITICAL";
        } else if (this.dynamicPriority >= 70.0) {
            this.severity = "P1_HIGH";
        } else if (this.dynamicPriority >= 40.0) {
            this.severity = "P2_MEDIUM";
        } else {
            this.severity = "P3_LOW";
        }
    }

    public void clearUpdatedFlag() {
        this.justUpdated = false;
    }

    // Getters
    public String getId() { return id; }
    public String getDeduplicationKey() { return deduplicationKey; }
    public String getSource() { return source; }
    public String getRoutingKey() { return routingKey; }
    public String getTitle() { return title; }
    public String getPayload() { return payload; }
    public String getCategory() { return category; }
    public String getSeverity() { return severity; }
    public double getBaseWeight() { return baseWeight; }
    public int getFrequency() { return frequency; }
    public long getFirstSeenMs() { return firstSeenMs; }
    public long getLastSeenMs() { return lastSeenMs; }
    public double getDynamicPriority() { return dynamicPriority; }
    public boolean isJustUpdated() { return justUpdated; }

    @Override
    public String toString() {
        return String.format("AlertNode[%s | key=%s | freq=%d | score=%.2f | sev=%s]",
                id, deduplicationKey, frequency, dynamicPriority, severity);
    }
}
