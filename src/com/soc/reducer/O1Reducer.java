package com.soc.reducer;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * O1Reducer: A constant-time O(1) notification deduplication engine combining a
 * {@link ConcurrentHashMap} with a custom thread-safe Doubly Linked List.
 * 
 * <p>Problem Solved:
 * When a high-volume notification storm arrives (e.g., WhatsApp chat flood, sensor bounce,
 * or brute-force telemetry), querying and updating repetitive alerts must NOT degrade to O(N).
 * This engine guarantees:
 * <ul>
 *   <li><b>O(1) Lookup:</b> Directly locate an existing alert record via its unique deduplication key.</li>
 *   <li><b>O(1) Frequency & Priority Escalation:</b> Increment repetition counter and recompute dynamic score.</li>
 *   <li><b>O(1) Recency Reordering:</b> Unlink and splice the active node to the head of the Doubly Linked List.</li>
 *   <li><b>O(1) Capacity Eviction:</b> Evict oldest idle alerts from the tail when max capacity is reached.</li>
 * </ul>
 * </p>
 * 
 * <p>Concurrency Guarantee:
 * Read lookups leverage lock-free ConcurrentHashMap bucket traversal, while structural DLL
 * pointer modifications are synchronized via a low-contention {@link ReentrantLock}.
 * </p>
 */
public class O1Reducer {

    /**
     * Ingestion result metadata indicating whether an incoming event was a duplicate or fresh.
     */
    public record ReductionResult(
            boolean isDuplicate,
            AlertNode node,
            int previousFrequency,
            int currentFrequency
    ) {}

    private final int maxCapacity;
    private final ConcurrentHashMap<String, AlertNode> nodeMap;
    private final ReentrantLock listLock;

    // Sentinel dummy nodes to eliminate boundary condition checks
    private final AlertNode dummyHead;
    private final AlertNode dummyTail;

    private int currentSize;
    private final AtomicLong totalIngested;
    private final AtomicLong totalDuplicatesCollapsed;

    /**
     * Initializes the O1Reducer with a maximum cache capacity.
     * 
     * @param maxCapacity Maximum unique active alerts maintained before tail eviction.
     * 
     * Time Complexity: O(1)
     * Space Complexity: O(1) auxiliary initial allocation.
     */
    public O1Reducer(int maxCapacity) {
        if (maxCapacity <= 0) {
            throw new IllegalArgumentException("Capacity must be positive: " + maxCapacity);
        }
        this.maxCapacity = maxCapacity;
        this.nodeMap = new ConcurrentHashMap<>(Math.min(maxCapacity, 4096));
        this.listLock = new ReentrantLock();

        // Initialize sentinel nodes
        this.dummyHead = new AlertNode(
                new Notification("__HEAD__", "", "", "", "", 0L, "__HEAD__"),
                RuleMetadata.DEFAULT_FALLBACK
        );
        this.dummyTail = new AlertNode(
                new Notification("__TAIL__", "", "", "", "", 0L, "__TAIL__"),
                RuleMetadata.DEFAULT_FALLBACK
        );

        this.dummyHead.next = this.dummyTail;
        this.dummyTail.prev = this.dummyHead;

        this.currentSize = 0;
        this.totalIngested = new AtomicLong(0);
        this.totalDuplicatesCollapsed = new AtomicLong(0);
    }

    /**
     * Default constructor with 5,000 alert slots.
     */
    public O1Reducer() {
        this(5000);
    }

    /**
     * Ingests an incoming notification, deduplicating repetitive alerts in O(1) time
     * or creating a new entry at the head in O(1) time.
     * 
     * @param notification Ingested raw notification
     * @param rule         Matched rule metadata from TopicTrie
     * @return {@link ReductionResult} detailing deduplication outcome and alert state
     * 
     * Time Complexity: O(1) average time.
     * Space Complexity: O(1) heap allocation (only when allocating new node).
     */
    public ReductionResult ingest(Notification notification, RuleMetadata rule) {
        Objects.requireNonNull(notification, "Notification cannot be null");
        Objects.requireNonNull(rule, "Rule cannot be null");

        totalIngested.incrementAndGet();
        String dedupKey = notification.deduplicationKey();

        listLock.lock();
        try {
            AlertNode existingNode = nodeMap.get(dedupKey);

            if (existingNode != null) {
                // CASE 1: Duplicate Alert Encountered (e.g. WhatsApp spam message #45)
                int prevFreq = existingNode.getFrequency();
                existingNode.recordDuplicateOccurrence(notification);
                int currFreq = existingNode.getFrequency();

                // Move node to head of Doubly Linked List in O(1) time
                moveToHead(existingNode);

                totalDuplicatesCollapsed.incrementAndGet();
                return new ReductionResult(true, existingNode, prevFreq, currFreq);
            } else {
                // CASE 2: Brand New Unique Alert
                AlertNode newNode = new AlertNode(notification, rule);

                // Capacity Guard: Evict least recently updated alert from tail in O(1)
                if (currentSize >= maxCapacity) {
                    evictTail();
                }

                // Add new node directly after dummyHead in O(1) time
                addToHead(newNode);
                nodeMap.put(dedupKey, newNode);
                currentSize++;

                return new ReductionResult(false, newNode, 0, 1);
            }
        } finally {
            listLock.unlock();
        }
    }

    /**
     * Moves an existing node to the front of the Doubly Linked List.
     * Relinks adjacent nodes in O(1) time and places node immediately after dummyHead.
     * 
     * @param node The node to promote to MRU (Most Recently Used) position
     * 
     * Time Complexity: O(1)
     * Space Complexity: O(1)
     */
    private void moveToHead(AlertNode node) {
        removeNode(node);
        addToHead(node);
    }

    /**
     * Inserts a node immediately after dummyHead in O(1) time.
     * 
     * @param node The node to prepend
     * 
     * Time Complexity: O(1)
     * Space Complexity: O(1)
     */
    private void addToHead(AlertNode node) {
        node.prev = dummyHead;
        node.next = dummyHead.next;

        dummyHead.next.prev = node;
        dummyHead.next = node;
    }

    /**
     * Unlinks an existing node from its current position in the Doubly Linked List in O(1) time.
     * 
     * @param node The node to detach
     * 
     * Time Complexity: O(1)
     * Space Complexity: O(1)
     */
    private void removeNode(AlertNode node) {
        if (node.prev != null) {
            node.prev.next = node.next;
        }
        if (node.next != null) {
            node.next.prev = node.prev;
        }
        node.prev = null;
        node.next = null;
    }

    /**
     * Evicts the oldest, least recently updated alert node adjacent to dummyTail in O(1) time.
     * Removes the node from both the Doubly Linked List and the ConcurrentHashMap.
     * 
     * @return Evicted {@link AlertNode}, or null if list was empty
     * 
     * Time Complexity: O(1)
     * Space Complexity: O(1)
     */
    private AlertNode evictTail() {
        if (currentSize == 0 || dummyTail.prev == dummyHead) {
            return null;
        }

        AlertNode leastRecent = dummyTail.prev;
        removeNode(leastRecent);
        nodeMap.remove(leastRecent.getDeduplicationKey());
        currentSize--;

        return leastRecent;
    }

    /**
     * Retrieves an alert node in O(1) time without modifying recency position.
     * 
     * @param deduplicationKey Key to look up
     * @return The alert node, or null if absent
     * 
     * Time Complexity: O(1)
     * Space Complexity: O(1)
     */
    public AlertNode get(String deduplicationKey) {
        return nodeMap.get(deduplicationKey);
    }

    /**
     * Returns a snapshot of the most recently updated alerts in recency order from head to tail.
     * 
     * @param limit Maximum number of alerts to return
     * @return List of AlertNodes from newest to oldest
     * 
     * Time Complexity: O(K) where K = min(limit, currentSize).
     * Space Complexity: O(K) for snapshot array list.
     */
    public List<AlertNode> getRecentSnapshot(int limit) {
        listLock.lock();
        try {
            int capacity = Math.min(limit, currentSize);
            List<AlertNode> snapshot = new ArrayList<>(capacity);

            AlertNode curr = dummyHead.next;
            while (curr != dummyTail && snapshot.size() < limit) {
                snapshot.add(curr);
                curr = curr.next;
            }

            return snapshot;
        } finally {
            listLock.unlock();
        }
    }

    /**
     * Returns the current number of unique active alert groups in the reducer.
     * 
     * Time Complexity: O(1)
     * Space Complexity: O(1)
     */
    public int size() {
        listLock.lock();
        try {
            return currentSize;
        } finally {
            listLock.unlock();
        }
    }

    /**
     * Resets the entire reducer state, clearing all nodes and counters.
     * 
     * Time Complexity: O(N) to dereference nodes for GC.
     * Space Complexity: O(1).
     */
    public void clear() {
        listLock.lock();
        try {
            nodeMap.clear();
            dummyHead.next = dummyTail;
            dummyTail.prev = dummyHead;
            currentSize = 0;
            totalIngested.set(0);
            totalDuplicatesCollapsed.set(0);
        } finally {
            listLock.unlock();
        }
    }

    public long getTotalIngested() {
        return totalIngested.get();
    }

    public long getTotalDuplicatesCollapsed() {
        return totalDuplicatesCollapsed.get();
    }

    /**
     * Computes the current deduplication efficiency percentage:
     * (totalDuplicatesCollapsed / totalIngested) * 100
     * 
     * Time Complexity: O(1)
     * Space Complexity: O(1)
     */
    public double getDeduplicationEfficiency() {
        long ingested = totalIngested.get();
        if (ingested == 0) return 0.0;
        return ((double) totalDuplicatesCollapsed.get() / (double) ingested) * 100.0;
    }
}
