package org.apache.nifi.pql.evaluation.extraction;

import org.apache.nifi.pql.evaluation.OperandEvaluator;
import org.apache.nifi.provenance.ProvenanceEventRecord;

public class TimestampEvaluator implements OperandEvaluator<Long> {

	@Override
	public Long evaluate(final ProvenanceEventRecord record) {
		if ( record == null ) {
			return null;
		}
		return record.getEventTime();
	}

	@Override
	public Class<Long> getType() {
		return Long.class;
	}

	@Override
	public int getEvaluatorType() {
		return org.apache.nifi.pql.ProvenanceQueryParser.TIMESTAMP;
	}

}
