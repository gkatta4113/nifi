package org.apache.nifi.pql.groups;

import java.util.ArrayList;
import java.util.List;

import org.apache.nifi.pql.evaluation.RecordEvaluator;
import org.apache.nifi.provenance.ProvenanceEventRecord;

public class Grouper {

	public static Group group(final ProvenanceEventRecord record, final List<RecordEvaluator<?>> evaluators) {
		final List<Object> values = new ArrayList<>(evaluators.size());
		for ( final RecordEvaluator<?> evaluator : evaluators ) {
			values.add(evaluator.evaluate(record));
		}
		
		return new Group(values);
	}
	
}
