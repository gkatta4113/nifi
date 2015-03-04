package org.apache.nifi.pql.evaluation.comparison;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import org.apache.nifi.pql.evaluation.BooleanEvaluator;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;

public class RecordTypeEvaluator implements BooleanEvaluator {
	private final Set<ProvenanceEventType> types;
	
	public RecordTypeEvaluator(final Set<ProvenanceEventType> types) {
		this.types = new HashSet<>(types);
	}
	
	@Override
	public Boolean evaluate(final ProvenanceEventRecord record) {
		return types.contains(record.getEventType());
	}

	@Override
	public BooleanEvaluator negate() {
		final Set<ProvenanceEventType> negatedTypes = EnumSet.complementOf(EnumSet.copyOf(types));
		return new RecordTypeEvaluator(negatedTypes);
	}

	@Override
	public int getEvaluatorType() {
		return org.apache.nifi.pql.ProvenanceQueryParser.TYPE;
	}

}
