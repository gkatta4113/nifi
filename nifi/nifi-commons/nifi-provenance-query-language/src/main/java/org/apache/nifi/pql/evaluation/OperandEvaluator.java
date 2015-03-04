package org.apache.nifi.pql.evaluation;

public interface OperandEvaluator<T> extends RecordEvaluator<T> {

	Class<T> getType();
	
}
