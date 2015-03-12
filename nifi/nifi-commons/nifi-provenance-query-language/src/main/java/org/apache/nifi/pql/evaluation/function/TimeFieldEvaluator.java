package org.apache.nifi.pql.evaluation.function;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

import org.apache.nifi.pql.evaluation.OperandEvaluator;
import org.apache.nifi.provenance.ProvenanceEventRecord;

public class TimeFieldEvaluator implements OperandEvaluator<Long> {
	private final OperandEvaluator<Long> timeExtractor;
	private final int evaluatorType;
	
	private final List<Integer> fieldsToClear = new ArrayList<>();
	
	public TimeFieldEvaluator(final OperandEvaluator<Long> timeExtractor, final int timeField, final int evaluatorType) {
		this.timeExtractor = timeExtractor;
		this.evaluatorType = evaluatorType;
		
		// note the case statements below are designed to "bleed through."
		// I.e., if time field is YEAR, we want to clear all of the fields starting with month.
		switch (timeField) {
			case Calendar.YEAR:
				fieldsToClear.add(Calendar.MONTH);
			case Calendar.MONTH:
			    fieldsToClear.add(Calendar.DAY_OF_MONTH);
			case Calendar.DAY_OF_MONTH:
				fieldsToClear.add(Calendar.HOUR);
			case Calendar.HOUR:
				fieldsToClear.add(Calendar.MINUTE);
			case Calendar.MINUTE:
				fieldsToClear.add(Calendar.SECOND);
			default:
				fieldsToClear.add(Calendar.MILLISECOND);
		}
	}
	
	@Override
	public Long evaluate(ProvenanceEventRecord record) {
		final Long epochMillis = timeExtractor.evaluate(record);
		if ( epochMillis == null ) {
			return null;
		}
		
		final Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		cal.setTimeInMillis(epochMillis);
		for ( final Integer field : fieldsToClear ) {
			cal.set(field, 0);
		}
		return Long.valueOf(cal.getTimeInMillis());
	}

	@Override
	public Class<Long> getType() {
		return Long.class;
	}

	@Override
	public int getEvaluatorType() {
		return evaluatorType;
	}

}
