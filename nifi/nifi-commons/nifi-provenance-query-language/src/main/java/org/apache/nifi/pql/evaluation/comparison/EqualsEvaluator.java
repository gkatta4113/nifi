package org.apache.nifi.pql.evaluation.comparison;

import org.apache.nifi.pql.evaluation.BooleanEvaluator;
import org.apache.nifi.pql.evaluation.OperandEvaluator;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;

public class EqualsEvaluator implements BooleanEvaluator {

	private final OperandEvaluator<?> lhs;
	private final OperandEvaluator<?> rhs;
	private final boolean negated;
	private final String alias;
	
	public EqualsEvaluator(final OperandEvaluator<?> lhs, final OperandEvaluator<?> rhs) {
		this(lhs, rhs, false, null);
	}

	public EqualsEvaluator(final OperandEvaluator<?> lhs, final OperandEvaluator<?> rhs, final String alias) {
		this(lhs, rhs, false, alias);
	}

	public EqualsEvaluator(final OperandEvaluator<?> lhs, final OperandEvaluator<?> rhs, final boolean negated) {
		this(lhs, rhs, negated, null);
	}
	
	public EqualsEvaluator(final OperandEvaluator<?> lhs, final OperandEvaluator<?> rhs, final boolean negated, final String alias) {
		this.lhs = lhs;
		this.rhs = rhs;
		this.negated = negated;
		this.alias = alias;
	}
	
	public OperandEvaluator<?> getLHS() {
		return lhs;
	}
	
	public OperandEvaluator<?> getRHS() {
		return rhs;
	}
	
	public Boolean evaluate(final ProvenanceEventRecord record) {
		Object lhsValue = lhs.evaluate(record);
		Object rhsValue = rhs.evaluate(record);
		
		if ( lhsValue == null || rhsValue == null ) {
			return false;
		}
		
		if ( lhsValue instanceof ProvenanceEventType ) {
			lhsValue = ((ProvenanceEventType) lhsValue).name();
		}
		if ( rhsValue instanceof ProvenanceEventType ) {
			rhsValue = ((ProvenanceEventType) rhsValue).name();
		}
		
		final boolean equal = lhsValue.equals(rhsValue);
		return negated ? !equal : equal;
	}

	public BooleanEvaluator negate() {
		return new EqualsEvaluator(lhs, rhs, !negated, alias);
	}

	@Override
	public String toString() {
		return alias == null ? lhs.toString() + "=" + rhs.toString() : alias;
	}
	
	@Override
	public int getEvaluatorType() {
		return org.apache.nifi.pql.ProvenanceQueryParser.EQUALS;
	}
}
