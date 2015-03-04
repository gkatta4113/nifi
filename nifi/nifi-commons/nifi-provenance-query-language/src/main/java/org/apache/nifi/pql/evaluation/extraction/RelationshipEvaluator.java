package org.apache.nifi.pql.evaluation.extraction;

import org.apache.nifi.pql.evaluation.OperandEvaluator;
import org.apache.nifi.provenance.ProvenanceEventRecord;

public class RelationshipEvaluator implements OperandEvaluator<String> {

	@Override
	public String evaluate(final ProvenanceEventRecord record) {
		return record.getRelationship();
	}

	@Override
	public Class<String> getType() {
		return String.class;
	}

	@Override
	public int getEvaluatorType() {
		return org.apache.nifi.pql.ProvenanceQueryParser.RELATIONSHIP;
	}
	
}
