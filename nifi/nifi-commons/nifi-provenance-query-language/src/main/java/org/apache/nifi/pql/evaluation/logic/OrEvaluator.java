package org.apache.nifi.pql.evaluation.logic;

import org.apache.nifi.pql.evaluation.BooleanEvaluator;
import org.apache.nifi.provenance.ProvenanceEventRecord;

public class OrEvaluator implements BooleanEvaluator {
	private final BooleanEvaluator lhs;
	private final BooleanEvaluator rhs;
	
	public OrEvaluator(final BooleanEvaluator lhs, final BooleanEvaluator rhs) {
		this.lhs = lhs;
		this.rhs = rhs;
	}
	
	@Override
	public Boolean evaluate(final ProvenanceEventRecord record) {
		return lhs.evaluate(record) || rhs.evaluate(record);
	}

	@Override
	public BooleanEvaluator negate() {
		return new AndEvaluator(lhs.negate(), rhs.negate());
	}

	public BooleanEvaluator getLHS() {
		return lhs;
	}
	
	public BooleanEvaluator getRHS() {
		return rhs;
	}
	
	
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		if ( lhs instanceof AndEvaluator || lhs instanceof OrEvaluator ) {
			sb.append("(").append(lhs.toString()).append(")");
		} else {
			sb.append(lhs.toString());
		}
		
		sb.append(" | ");
		
		if ( rhs instanceof AndEvaluator || rhs instanceof OrEvaluator ) {
			sb.append("(").append(rhs.toString()).append(")");
		} else {
			sb.append(rhs.toString());
		}

		return sb.toString();
	}
	
	@Override
	public int getEvaluatorType() {
		return org.apache.nifi.pql.ProvenanceQueryParser.OR;
	}

}
