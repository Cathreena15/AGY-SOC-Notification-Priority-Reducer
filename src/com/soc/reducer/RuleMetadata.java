package com.soc.reducer;

/**
 * Metadata record associated with a routing key rule in the TopicTrie.
 * Represents categorization, baseline priority weight, severity classification,
 * and rule description.
 * 
 * @param category    Functional operational domain (e.g., "CYBER_DEFENSE", "INFRASTRUCTURE", "COMMUNICATIONS")
 * @param severity    SLA priority tier ("P0_CRITICAL", "P1_HIGH", "P2_MEDIUM", "P3_LOW")
 * @param baseWeight  Base priority weight score on a scale from 0.0 to 100.0
 * @param description Brief contextual description of the rule
 */
public record RuleMetadata(
        String category,
        String severity,
        double baseWeight,
        String description
) {
    public static final RuleMetadata DEFAULT_FALLBACK = new RuleMetadata(
            "TELEMETRY_DEFAULT",
            "P3_LOW",
            20.0,
            "Default fallback routing rule for unclassified telemetry"
    );
}
