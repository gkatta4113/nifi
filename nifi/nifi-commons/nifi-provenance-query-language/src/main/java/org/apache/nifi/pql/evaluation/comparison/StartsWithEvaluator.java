package org.apache.nifi.pql.evaluation.comparison;

import org.apache.nifi.pql.evaluation.BooleanEvaluator;
import org.apache.nifi.pql.evaluation.OperandEvaluator;
import org.apache.nifi.provenance.ProvenanceEventRecord;

public class StartsWithEvaluator implements BooleanEvaluator {

	private final boolean negated;
	private final OperandEvaluator<?> lhs;
	private final OperandEvaluator<?> rhs;
	
	public StartsWithEvaluator(final OperandEvaluator<?> lhs, final OperandEvaluator<?> rhs) {
		this(lhs, rhs, false);
	}
	
	public StartsWithEvaluator(final OperandEvaluator<?> lhs, final OperandEvaluator<?> rhs, final boolean negated) {
		this.lhs = lhs;
		this.rhs = rhs;
		this.negated = negated;
	}
	
	@Override
	public Boolean evaluate(final ProvenanceEventRecord record) {
		final Object lhsValue = lhs.evaluate(record);
		final Object rhsValue = rhs.evaluate(record);
		
		if ( lhsValue == null || rhsValue == null ) {
			return false;
		}
		
		final String lhsString = lhsValue.toString();
		final String rhsString = rhsValue.toString();
		
		final boolean startsWith = lhsString.startsWith(rhsString);
		return negated ? !startsWith : startsWith;
	}

	public OperandEvaluator<?> getLHS() {
		return lhs;
	}
	
	public OperandEvaluator<?> getRHS() {
		return rhs;
	}
	
	@Override
	public BooleanEvaluator negate() {
		return new StartsWithEvaluator(lhs, rhs, !negated);
	}

	@Override
	public int getEvaluatorType() {
		return org.apache.nifi.pql.ProvenanceQueryParser.STARTS_WITH;
	}

}
