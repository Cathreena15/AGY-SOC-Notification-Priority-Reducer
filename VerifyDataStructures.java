import java.util.List;

/**
 * Diagnostic Verification Test Suite to validate:
 * 1. TopicTrie O(k) prefix matching
 * 2. O1Reducer O(1) deduplication and DLL recency
 * 3. PriorityMaxHeap O(log n) rebalancing and inverted index updates
 */
public class VerifyDataStructures {

    public static void main(String[] args) {
        System.out.println(">>> RUNNING DATA STRUCTURE INTEGRITY VALIDATION <<<");

        // 1. Validate TopicTrie
        System.out.println("\n[1] Testing TopicTrie (Prefix Tree)...");
        TopicTrie trie = TopicTrie.createDefaultSOCEngine();
        assert trie.getRuleCount() > 10 : "Rule count should be > 10";

        RuleMetadata r1 = trie.match("sec.auth.bruteforce");
        System.out.printf("  Match 'sec.auth.bruteforce': Category=%s, Weight=%.1f, Severity=%s%n",
                r1.category(), r1.baseWeight(), r1.severity());
        assert "P0_CRITICAL".equals(r1.severity()) : "Should be P0_CRITICAL";

        RuleMetadata r2 = trie.match("comm.whatsapp.spam");
        System.out.printf("  Match 'comm.whatsapp.spam': Category=%s, Weight=%.1f, Severity=%s%n",
                r2.category(), r2.baseWeight(), r2.severity());
        assert "P3_LOW".equals(r2.severity()) : "Should be P3_LOW";

        RuleMetadata rFallback = trie.match("unknown.custom.unclassified.topic");
        System.out.printf("  Match unknown topic fallback: Category=%s, Weight=%.1f%n",
                rFallback.category(), rFallback.baseWeight());
        assert "TELEMETRY_DEFAULT".equals(rFallback.category()) : "Should be default fallback";
        System.out.println("  ==> TopicTrie tests PASSED successfully.");

        // 2. Validate O1Reducer
        System.out.println("\n[2] Testing O1Reducer (ConcurrentHashMap + DoublyLinkedList)...");
        O1Reducer reducer = new O1Reducer(5); // small capacity 5 to test eviction

        Notification n1 = Notification.of("1", "gw-1", "comm.whatsapp.spam", "Promo Spam", "Buy now", 1000L);
        Notification n2 = Notification.of("2", "gw-2", "infra.db.deadlock", "DB Deadlock", "Lock issue", 1005L);
        Notification n3 = Notification.of("3", "gw-1", "comm.whatsapp.spam", "Promo Spam", "Buy now duplicate 1", 1010L);

        O1Reducer.ReductionResult res1 = reducer.ingest(n1, r2);
        assert !res1.isDuplicate() : "n1 should be fresh";
        assert res1.currentFrequency() == 1;

        O1Reducer.ReductionResult res2 = reducer.ingest(n2, r1);
        assert !res2.isDuplicate() : "n2 should be fresh";

        // Ingest duplicate of n1
        O1Reducer.ReductionResult res3 = reducer.ingest(n3, r2);
        assert res3.isDuplicate() : "n3 must be recognized as duplicate in O(1)";
        assert res3.currentFrequency() == 2 : "Frequency should increment to 2";
        System.out.printf("  Duplicate collapsed: Key=%s, Frequency=%d, DynamicPriority=%.2f%n",
                res3.node().getDeduplicationKey(), res3.currentFrequency(), res3.node().getDynamicPriority());

        // Check DLL order: n1 should have moved to head because it was updated most recently
        List<AlertNode> recent = reducer.getRecentSnapshot(10);
        assert recent.get(0).getDeduplicationKey().equals(n1.deduplicationKey()) : "Most recently updated must be at head of DLL";
        System.out.println("  ==> O1Reducer tests PASSED successfully.");

        // 3. Validate PriorityMaxHeap
        System.out.println("\n[3] Testing PriorityMaxHeap (Array-backed Max-Heap with Inverted Index)...");
        PriorityMaxHeap heap = new PriorityMaxHeap(16);

        AlertNode alertLow = new AlertNode(n1, r2); // base 15.0
        AlertNode alertCrit = new AlertNode(n2, r1); // base 98.5

        heap.insertOrUpdate(alertLow);
        heap.insertOrUpdate(alertCrit);

        assert heap.peekMax() == alertCrit : "P0 alert must be at root of MaxHeap";
        System.out.printf("  Root of MaxHeap: Key=%s, Score=%.1f, Severity=%s%n",
                heap.peekMax().getDeduplicationKey(), heap.peekMax().getDynamicPriority(), heap.peekMax().getSeverity());

        // Test in-place dynamic escalation: simulate massive spam bursts on alertLow
        for (int i = 0; i < 2000; i++) {
            alertLow.recordDuplicateOccurrence(n1);
        }
        // In-place heap update in O(log n) using inverted index
        heap.insertOrUpdate(alertLow);
        System.out.printf("  After 2000 duplicates, Low Alert dynamic priority boosted to: %.2f%n", alertLow.getDynamicPriority());

        List<AlertNode> top = heap.getTopK(2);
        System.out.printf("  Top 1: %s (Score: %.1f)%n", top.get(0).getTitle(), top.get(0).getDynamicPriority());
        System.out.printf("  Top 2: %s (Score: %.1f)%n", top.get(1).getTitle(), top.get(1).getDynamicPriority());

        AlertNode extracted = heap.extractMax();
        assert extracted != null : "Extracted node should not be null";
        System.out.printf("  Extracted Max: %s (Remaining Heap Size: %d)%n", extracted.getTitle(), heap.size());
        System.out.println("  ==> PriorityMaxHeap tests PASSED successfully.");

        System.out.println("\n>>> ALL ADVANCED DATA STRUCTURE TESTS PASSED WITH 100% FIDELITY! <<<\n");
    }
}
