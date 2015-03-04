package org.apache.nifi.pql.evaluation.literals;

import org.apache.nifi.pql.evaluation.OperandEvaluator;
import org.apache.nifi.provenance.ProvenanceEventRecord;

public class LongLiteralEvaluator implements OperandEvaluator<Long> {
	private final Long value;
	
	public LongLiteralEvaluator(final Long value) {
		this.value = value;
	}
	
	@Override
	public Long evaluate(final ProvenanceEventRecord record) {
		return value;
	}

	@Override
	public Class<Long> getType() {
		return Long.class;
	}

	@Override
	public int getEvaluatorType() {
		return org.apache.nifi.pql.ProvenanceQueryParser.NUMBER;
	}
}
