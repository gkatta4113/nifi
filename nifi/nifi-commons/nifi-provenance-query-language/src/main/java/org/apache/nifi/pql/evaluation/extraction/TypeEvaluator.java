package org.apache.nifi.pql.evaluation.extraction;

import org.apache.nifi.pql.evaluation.OperandEvaluator;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;

public class TypeEvaluator implements OperandEvaluator<ProvenanceEventType> {

	@Override
	public ProvenanceEventType evaluate(final ProvenanceEventRecord record) {
		return record.getEventType();
	}

	@Override
	public Class<ProvenanceEventType> getType() {
		return ProvenanceEventType.class;
	}

	@Override
	public int getEvaluatorType() {
		return org.apache.nifi.pql.ProvenanceQueryParser.TYPE;
	}

}
