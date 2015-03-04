package org.apache.nifi.pql.evaluation.literals;

import org.apache.nifi.pql.evaluation.OperandEvaluator;
import org.apache.nifi.provenance.ProvenanceEventRecord;

public class StringLiteralEvaluator implements OperandEvaluator<String> {
	private final String value;
	
	public StringLiteralEvaluator(final String value) {
		this.value = value;
	}
	
	@Override
	public String evaluate(final ProvenanceEventRecord record) {
		return value;
	}

	@Override
	public Class<String> getType() {
		return String.class;
	}

	@Override
	public String toString() {
		return value;
	}
	
	@Override
	public int getEvaluatorType() {
		return org.apache.nifi.pql.ProvenanceQueryParser.STRING_LITERAL;
	}

}
