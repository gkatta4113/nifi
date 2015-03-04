package org.apache.nifi.pql.evaluation;

public interface BooleanEvaluator extends RecordEvaluator<Boolean> {
	BooleanEvaluator negate();
}
