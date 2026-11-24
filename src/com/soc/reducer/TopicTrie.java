package com.soc.reducer;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * TopicTrie: A high-performance character-by-character Prefix Tree (Trie)
 * designed for real-time routing key classification and priority weight assignment.
 * 
 * <p>In distributed message brokers and SOC pipelines, routing keys follow hierarchical
 * conventions such as {@code "sec.auth.bruteforce"} or {@code "sys.battery.low"}.
 * This Trie parses incoming routing keys character-by-character in <b>O(k)</b> time
 * (where {@code k} is the length of the routing key) to extract the most specific
 * matching rule and its associated {@link RuleMetadata}.</p>
 * 
 * <p>Thread-Safety: Read/Write thread safety is enforced via {@link ReentrantReadWriteLock},
 * enabling massive concurrent virtual-thread read operations without blocking.</p>
 */
public class TopicTrie {

    /**
     * Internal node of the Prefix Tree.
     */
    static class TrieNode {
        final Map<Character, TrieNode> children = new HashMap<>(4);
        boolean isEndOfRule = false;
        RuleMetadata rule = null;
        String registeredPattern = null;

        TrieNode() {}
    }

    private final TrieNode root;
    private final ReentrantReadWriteLock rwLock;
    private int totalRulesRegistered;

    /**
     * Initializes an empty TopicTrie with a root sentinel node.
     * 
     * Time Complexity: O(1)
     * Space Complexity: O(1)
     */
    public TopicTrie() {
        this.root = new TrieNode();
        this.rwLock = new ReentrantReadWriteLock();
        this.totalRulesRegistered = 0;
    }

    /**
     * Inserts a routing key pattern along with its priority metadata into the Trie.
     * Traverses or allocates nodes character-by-character.
     * 
     * @param pattern  Hierarchical routing key pattern (e.g., "sys.battery.low")
     * @param metadata Priority and category metadata to bind to this pattern
     * 
     * Time Complexity: O(k) where k is the length of the pattern string.
     * Space Complexity: O(k) in the worst case if no characters share existing prefixes.
     */
    public void insertRule(String pattern, RuleMetadata metadata) {
        Objects.requireNonNull(pattern, "Pattern cannot be null");
        Objects.requireNonNull(metadata, "Metadata cannot be null");

        rwLock.writeLock().lock();
        try {
            TrieNode current = root;
            int length = pattern.length();

            for (int i = 0; i < length; i++) {
                char ch = pattern.charAt(i);
                current = current.children.computeIfAbsent(ch, c -> new TrieNode());
            }

            if (!current.isEndOfRule) {
                totalRulesRegistered++;
            }
            current.isEndOfRule = true;
            current.rule = metadata;
            current.registeredPattern = pattern;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Evaluates an incoming routing key character-by-character to assign priority metadata.
     * Implements Longest Prefix Matching (LPM): if multiple rules match along the path
     * (e.g., "sys" and "sys.battery.low"), the deepest (most specific) rule takes precedence.
     * If no rule matches, returns {@link RuleMetadata#DEFAULT_FALLBACK}.
     * 
     * @param routingKey The incoming routing key (e.g., "sys.battery.low.sensor4")
     * @return The matched {@link RuleMetadata}, or default fallback if unclassified
     * 
     * Time Complexity: O(k) where k is the length of the routingKey string.
     * Space Complexity: O(1) auxiliary space (zero heap allocation during search).
     */
    public RuleMetadata match(String routingKey) {
        if (routingKey == null || routingKey.isEmpty()) {
            return RuleMetadata.DEFAULT_FALLBACK;
        }

        rwLock.readLock().lock();
        try {
            TrieNode current = root;
            RuleMetadata bestMatch = null;
            int length = routingKey.length();

            for (int i = 0; i < length; i++) {
                char ch = routingKey.charAt(i);
                current = current.children.get(ch);
                if (current == null) {
                    break; // No further branch exists
                }
                if (current.isEndOfRule) {
                    bestMatch = current.rule; // Record deepest matched rule so far
                }
            }

            return (bestMatch != null) ? bestMatch : RuleMetadata.DEFAULT_FALLBACK;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Checks if a specific exact pattern exists in the Trie.
     * 
     * @param pattern Pattern to query
     * @return true if exact rule exists, false otherwise
     * 
     * Time Complexity: O(k) where k is pattern length.
     * Space Complexity: O(1) auxiliary space.
     */
    public boolean containsExactPattern(String pattern) {
        if (pattern == null) return false;

        rwLock.readLock().lock();
        try {
            TrieNode current = root;
            for (int i = 0; i < pattern.length(); i++) {
                char ch = pattern.charAt(i);
                current = current.children.get(ch);
                if (current == null) return false;
            }
            return current.isEndOfRule;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Returns total number of registered unique rules in the Trie.
     * 
     * Time Complexity: O(1)
     * Space Complexity: O(1)
     */
    public int getRuleCount() {
        rwLock.readLock().lock();
        try {
            return totalRulesRegistered;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Pre-populates the Trie with standard enterprise SOC and distributed systems rules.
     * Covers cyber defense, infrastructure, system telemetry, IoT sensors, and communications.
     * 
     * Time Complexity: O(R * K) where R is rule count and K is average pattern length.
     * Space Complexity: O(R * K) total tree node allocation.
     */
    public static TopicTrie createDefaultSOCEngine() {
        TopicTrie trie = new TopicTrie();

        // P0 CRITICAL Rules (Cyber Defense & Core Kernel)
        trie.insertRule("sec.auth.bruteforce", new RuleMetadata(
                "CYBER_DEFENSE", "P0_CRITICAL", 98.5, "Distributed credential stuffing or SSH brute force detected"));
        trie.insertRule("sec.ddos.synflood", new RuleMetadata(
                "CYBER_DEFENSE", "P0_CRITICAL", 99.0, "High-volume SYN flood breach detected on edge border gateway"));
        trie.insertRule("sec.rce.exploit", new RuleMetadata(
                "CYBER_DEFENSE", "P0_CRITICAL", 100.0, "Remote code execution attempt detected via zero-day signature"));
        trie.insertRule("sys.kernel.panic", new RuleMetadata(
                "SYSTEM_KERNEL", "P0_CRITICAL", 97.0, "Fatal kernel panic or unrecoverable hardware fault"));
        trie.insertRule("infra.db.deadlock", new RuleMetadata(
                "INFRASTRUCTURE", "P0_CRITICAL", 95.0, "Cluster-wide distributed database transaction deadlock"));

        // P1 HIGH Rules (Infrastructure & Sensors)
        trie.insertRule("infra.k8s.oomkill", new RuleMetadata(
                "INFRASTRUCTURE", "P1_HIGH", 85.0, "Critical container evicted due to memory exhaustion (OOM)"));
        trie.insertRule("infra.k8s.crashloop", new RuleMetadata(
                "INFRASTRUCTURE", "P1_HIGH", 82.0, "Pod ingress service in crash loop backoff state"));
        trie.insertRule("sec.firewall.drop", new RuleMetadata(
                "CYBER_DEFENSE", "P1_HIGH", 78.0, "Anomalous traffic block on perimeter security group"));
        trie.insertRule("sys.disk.full", new RuleMetadata(
                "SYSTEM_KERNEL", "P1_HIGH", 88.0, "Root filesystem disk space utilization exceeded 98%"));
        trie.insertRule("sys.battery.low", new RuleMetadata(
                "TELEMETRY", "P1_HIGH", 75.0, "Critical battery voltage threshold breached in edge drone node"));
        trie.insertRule("iot.sensor.temp.critical", new RuleMetadata(
                "IOT_SENSORS", "P1_HIGH", 84.0, "Data center server rack thermal runway warning"));

        // P2 MEDIUM Rules (Operational & Standard Telemetry)
        trie.insertRule("comm.slack.mention", new RuleMetadata(
                "COMMUNICATIONS", "P2_MEDIUM", 55.0, "High-priority direct mention on executive incident bridge"));
        trie.insertRule("comm.email.urgent", new RuleMetadata(
                "COMMUNICATIONS", "P2_MEDIUM", 50.0, "Urgent support ticket escalated by tier-1 enterprise customer"));
        trie.insertRule("sys.cpu.high", new RuleMetadata(
                "TELEMETRY", "P2_MEDIUM", 60.0, "CPU core saturation exceeded 85% for sustained 5 minutes"));
        trie.insertRule("iot.sensor.motion", new RuleMetadata(
                "IOT_SENSORS", "P2_MEDIUM", 45.0, "Perimeter security fence motion sensor trip"));

        // P3 LOW Rules (Bulk Messaging, Heartbeats, Chat Floods)
        trie.insertRule("comm.whatsapp.group", new RuleMetadata(
                "COMMUNICATIONS", "P3_LOW", 20.0, "Unmoderated bulk messaging activity in public broadcast group"));
        trie.insertRule("comm.whatsapp.spam", new RuleMetadata(
                "COMMUNICATIONS", "P3_LOW", 15.0, "Automated promotional WhatsApp bot spam blast"));
        trie.insertRule("telemetry.heartbeat", new RuleMetadata(
                "TELEMETRY", "P3_LOW", 10.0, "Routine cluster node keep-alive heartbeat signal"));
        trie.insertRule("iot.sensor.ping", new RuleMetadata(
                "IOT_SENSORS", "P3_LOW", 5.0, "Standard periodic sensor telemetry telemetry ping"));

        return trie;
    }
}
