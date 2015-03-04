package org.apache.nifi.pql.evaluation;

import org.apache.nifi.provenance.ProvenanceEventRecord;

public interface RecordEvaluator<T> {

	T evaluate(ProvenanceEventRecord record);
	
	int getEvaluatorType();
	
}
