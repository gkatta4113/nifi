package org.apache.nifi.pql.evaluation.extraction;

import org.apache.nifi.pql.evaluation.OperandEvaluator;
import org.apache.nifi.provenance.ProvenanceEventRecord;

public class TransitUriEvaluator implements OperandEvaluator<String> {

	@Override
	public String evaluate(final ProvenanceEventRecord record) {
		return record.getTransitUri();
	}

	@Override
	public Class<String> getType() {
		return String.class;
	}

	@Override
	public int getEvaluatorType() {
		return org.apache.nifi.pql.ProvenanceQueryParser.TRANSIT_URI;
	}

}
