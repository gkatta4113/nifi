package org.apache.nifi.pql.evaluation.comparison;

import org.apache.nifi.pql.evaluation.BooleanEvaluator;
import org.apache.nifi.pql.evaluation.OperandEvaluator;
import org.apache.nifi.provenance.ProvenanceEventRecord;

public class LessThanEvaluator implements BooleanEvaluator {
	private final OperandEvaluator<?> lhs;
	private final OperandEvaluator<?> rhs;
	private final boolean negated;
	
	public LessThanEvaluator(final OperandEvaluator<?> lhs, final OperandEvaluator<?> rhs) {
		this(lhs, rhs, false);
	}
	
	public LessThanEvaluator(final OperandEvaluator<?> lhs, final OperandEvaluator<?> rhs, final boolean negate) {
		this.lhs = lhs;
		this.rhs = rhs;
		this.negated = negate;
	}
	
	public OperandEvaluator<?> getLHS() {
		return lhs;
	}
	
	public OperandEvaluator<?> getRHS() {
		return rhs;
	}
	
	public Boolean evaluate(final ProvenanceEventRecord record) {
		final Long lhsValue = ConversionUtils.convertToLong(lhs.evaluate(record));
		final Long rhsValue = ConversionUtils.convertToLong(rhs.evaluate(record));
		
		if ( lhsValue == null || rhsValue == null ) {
			return false;
		}
		
		final boolean lessThan = lhsValue.longValue() < rhsValue.longValue();
		return negated ? !lessThan : lessThan;
	}

	public BooleanEvaluator negate() {
		return new GreaterThanEvaluator(lhs, rhs, !negated);
	}

	@Override
	public int getEvaluatorType() {
		return org.apache.nifi.pql.ProvenanceQueryParser.LT;
	}

}
