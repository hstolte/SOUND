package io.stolther.soundcheck.core;

import java.util.HashMap;
import java.util.Map;

public enum Outcome {
    SATISFIED(1),
    VIOLATED(2),
    INCONCLUSIVE(0);

    private final int value;

    private Outcome(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }


    private static final Map<Integer, Outcome> VALUE_TO_OUTCOME_MAP = new HashMap<>();

    static {
        for (Outcome outcome : values()) {
            VALUE_TO_OUTCOME_MAP.put(outcome.getValue(), outcome);
        }
    }

    public static Outcome fromValue(int value) {
        Outcome outcome = VALUE_TO_OUTCOME_MAP.get(value);
        if (outcome == null) {
            throw new IllegalArgumentException("Invalid value: " + value);
        }
        return outcome;
    }
}
