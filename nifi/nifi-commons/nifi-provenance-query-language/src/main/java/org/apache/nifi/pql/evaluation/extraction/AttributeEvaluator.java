package org.apache.nifi.pql.evaluation.extraction;

import org.apache.nifi.pql.evaluation.OperandEvaluator;
import org.apache.nifi.provenance.ProvenanceEventRecord;

public class AttributeEvaluator implements OperandEvaluator<String> {
	private final OperandEvaluator<String> attributeNameEvaluator;
	
	public AttributeEvaluator(final OperandEvaluator<String> attributeNameEvaluator) {
		this.attributeNameEvaluator = attributeNameEvaluator;
	}
	
	public OperandEvaluator<String> getAttributeNameEvaluator() {
		return attributeNameEvaluator;
	}
	
	@Override
	public String evaluate(final ProvenanceEventRecord record) {
		final String attributeName = attributeNameEvaluator.evaluate(record);
		if ( attributeName == null ) {
			return null;
		}
		
		return record.getAttribute(attributeName);
	}

	
	@Override
	public Class<String> getType() {
		return String.class;
	}
	
	@Override
	public String toString() {
		return attributeNameEvaluator.toString();
	}
	
	@Override
	public int getEvaluatorType() {
		return org.apache.nifi.pql.ProvenanceQueryParser.ATTRIBUTE;
	}
}
