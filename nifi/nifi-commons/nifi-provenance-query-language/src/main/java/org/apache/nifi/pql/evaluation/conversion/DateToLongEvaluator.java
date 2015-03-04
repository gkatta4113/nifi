package org.apache.nifi.pql.evaluation.conversion;

import org.apache.nifi.pql.evaluation.OperandEvaluator;
import org.apache.nifi.provenance.ProvenanceEventRecord;

public class DateToLongEvaluator implements OperandEvaluator<Long> {

	
	@Override
	public Long evaluate(ProvenanceEventRecord record) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getEvaluatorType() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Class<Long> getType() {
		// TODO Auto-generated method stub
		return null;
	}

}
