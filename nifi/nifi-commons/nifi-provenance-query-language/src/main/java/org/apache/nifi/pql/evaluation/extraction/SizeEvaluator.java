package org.apache.nifi.pql.evaluation.extraction;

import org.apache.nifi.pql.evaluation.OperandEvaluator;
import org.apache.nifi.provenance.ProvenanceEventRecord;

public class SizeEvaluator implements OperandEvaluator<Long> {

	@Override
	public Long evaluate(final ProvenanceEventRecord record) {
		return record.getFileSize();
	}

	@Override
	public Class<Long> getType() {
		return Long.class;
	}

	@Override
	public int getEvaluatorType() {
		return org.apache.nifi.pql.ProvenanceQueryParser.FILESIZE;
	}

}
