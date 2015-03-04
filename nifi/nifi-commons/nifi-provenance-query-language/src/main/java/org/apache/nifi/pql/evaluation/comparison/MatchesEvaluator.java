package org.apache.nifi.pql.evaluation.comparison;

import java.util.regex.Pattern;

import org.apache.nifi.pql.evaluation.BooleanEvaluator;
import org.apache.nifi.pql.evaluation.OperandEvaluator;
import org.apache.nifi.pql.evaluation.literals.StringLiteralEvaluator;
import org.apache.nifi.provenance.ProvenanceEventRecord;

public class MatchesEvaluator implements BooleanEvaluator {

	private final OperandEvaluator<?> lhs;
	private final OperandEvaluator<?> rhs;
	private final boolean negated;
	private final Pattern pattern;
	
	public MatchesEvaluator(final OperandEvaluator<?> lhs, final OperandEvaluator<?> rhs) {
		this(lhs, rhs, false);
	}
	
	
	public MatchesEvaluator(final OperandEvaluator<?> lhs, final OperandEvaluator<?> rhs, final boolean negated) {
		this.lhs = lhs;
		this.rhs = rhs;
		this.negated = negated;
		
		if ( rhs instanceof StringLiteralEvaluator ) {
			pattern = Pattern.compile(rhs.evaluate(null).toString());
		} else {
			pattern = null;
		}
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
		
		final String lhsString = lhsValue.toString();
		
		final Pattern compiled;
		if ( pattern == null ) {
			compiled = Pattern.compile(rhsValue.toString());
		} else {
			compiled = pattern;
		}
		
		final boolean matches = compiled.matcher(lhsString).matches();
		return negated ? !matches : matches;
	}

	public MatchesEvaluator negate() {
		return new MatchesEvaluator(lhs, rhs, !negated);
	}
	
	@Override
	public int getEvaluatorType() {
		return org.apache.nifi.pql.ProvenanceQueryParser.MATCHES;
	}

}
