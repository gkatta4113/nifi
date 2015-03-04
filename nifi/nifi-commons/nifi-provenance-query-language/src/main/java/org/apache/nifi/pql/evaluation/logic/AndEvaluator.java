package org.apache.nifi.pql.evaluation.logic;

import java.util.ArrayList;
import java.util.List;

import org.apache.nifi.pql.evaluation.BooleanEvaluator;
import org.apache.nifi.provenance.ProvenanceEventRecord;

public class AndEvaluator implements BooleanEvaluator {

	private final BooleanEvaluator lhs;
	private final BooleanEvaluator rhs;
	
	
	public AndEvaluator(final BooleanEvaluator lhs, final BooleanEvaluator rhs) {
		this.lhs = lhs;
		this.rhs = rhs;
	}
	
	@Override
	public Boolean evaluate(final ProvenanceEventRecord record) {
		return lhs.evaluate(record) && rhs.evaluate(record);
	}

	@Override
	public BooleanEvaluator negate() {
		return new OrEvaluator(lhs.negate(), rhs.negate());
	}

	public BooleanEvaluator getLHS() {
		return lhs;
	}
	
	public BooleanEvaluator getRHS() {
		return rhs;
	}
	
	/**
	 * Converts this AND tree to Disjunctive Normal Form (OR's of AND's)
	 * @return
	 */
	public BooleanEvaluator toDNF() {
		final List<BooleanEvaluator> rhsEvaluators = new ArrayList<>();
		final List<BooleanEvaluator> lhsEvaluators = new ArrayList<>();
		
		if ( rhs instanceof OrEvaluator ) {
			final OrEvaluator or = (OrEvaluator) rhs;
			rhsEvaluators.add(or.getLHS());
			rhsEvaluators.add(or.getRHS());
		} else if ( rhs instanceof AndEvaluator ) {
			rhsEvaluators.add( ((AndEvaluator) rhs).toDNF() );
		} else {
			rhsEvaluators.add(rhs);
		}
		
		if ( lhs instanceof OrEvaluator ) {
			final OrEvaluator or = (OrEvaluator) lhs;
			lhsEvaluators.add(or.getLHS());
			lhsEvaluators.add(or.getRHS());
		} else if ( lhs instanceof AndEvaluator ) {
			lhsEvaluators.add( ((AndEvaluator) lhs).toDNF() );
		} else {
			lhsEvaluators.add(lhs);
		}
		
		if ( rhsEvaluators.size() == 1 && lhsEvaluators.size() == 1 ) {
			return this;
		}
		
		final List<AndEvaluator> ands = new ArrayList<>();
		for ( final BooleanEvaluator l : lhsEvaluators ) {
			for ( final BooleanEvaluator r : rhsEvaluators ) {
				final AndEvaluator and = new AndEvaluator(l, r);
				ands.add(and);
			}
		}
		
		final AndEvaluator and1 = ands.get(0);
		final AndEvaluator and2 = ands.get(1);
		OrEvaluator or = new OrEvaluator(and1, and2);
		
		for (int i=2; i < ands.size(); i++) {
			final AndEvaluator ae = ands.get(i);
			or = new OrEvaluator(or, ae);
		}
		
		return or;
	}
	
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		if ( lhs instanceof AndEvaluator || lhs instanceof OrEvaluator ) {
			sb.append("(").append(lhs.toString()).append(")");
		} else {
			sb.append(lhs.toString());
		}
		
		sb.append(" & ");
		
		if ( rhs instanceof AndEvaluator || rhs instanceof OrEvaluator ) {
			sb.append("(").append(rhs.toString()).append(")");
		} else {
			sb.append(rhs.toString());
		}

		return sb.toString();
	}

	@Override
	public int getEvaluatorType() {
		return org.apache.nifi.pql.ProvenanceQueryParser.AND;
	}

}
