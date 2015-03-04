package org.apache.nifi.pql;

import static org.apache.nifi.pql.ProvenanceQueryParser.*;

import java.text.SimpleDateFormat;
import java.util.regex.Pattern;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.nifi.pql.evaluation.OperandEvaluator;
import org.apache.nifi.pql.evaluation.RecordEvaluator;
import org.apache.nifi.pql.evaluation.comparison.EqualsEvaluator;
import org.apache.nifi.pql.evaluation.comparison.GreaterThanEvaluator;
import org.apache.nifi.pql.evaluation.comparison.LessThanEvaluator;
import org.apache.nifi.pql.evaluation.comparison.MatchesEvaluator;
import org.apache.nifi.pql.evaluation.comparison.StartsWithEvaluator;
import org.apache.nifi.pql.evaluation.extraction.AttributeEvaluator;
import org.apache.nifi.pql.evaluation.literals.LongLiteralEvaluator;
import org.apache.nifi.pql.evaluation.literals.StringLiteralEvaluator;
import org.apache.nifi.pql.evaluation.logic.AndEvaluator;
import org.apache.nifi.pql.evaluation.logic.OrEvaluator;
import org.apache.nifi.provenance.SearchableFields;


public class LuceneTranslator {

	public static Query toLuceneQuery(final RecordEvaluator<Boolean> whereClause) {
	    if ( whereClause == null ) {
	        return new MatchAllDocsQuery();
	    }
	    
		final BooleanQuery query = new BooleanQuery();
		switch (whereClause.getEvaluatorType()) {
			case AND:
				final AndEvaluator and = (AndEvaluator) whereClause;
				query.add(toLuceneQuery(and.getLHS()), Occur.MUST);
				query.add(toLuceneQuery(and.getRHS()), Occur.MUST);
				break;
			case OR:
				final OrEvaluator or = (OrEvaluator) whereClause;
				query.add(toLuceneQuery(or.getLHS()), Occur.SHOULD);
				query.add(toLuceneQuery(or.getRHS()), Occur.SHOULD);
				query.setMinimumNumberShouldMatch(1);
				break;
			case GT: {
					final GreaterThanEvaluator gt = (GreaterThanEvaluator) whereClause;
					final OperandEvaluator<?> lhs = gt.getLHS();
					final OperandEvaluator<?> rhs = gt.getRHS();
					
					final String fieldName = getFieldName(lhs);
					if ( fieldName != null ) {
						Long rhsValue = null;
						if ( rhs.getEvaluatorType() == NUMBER ) {
							rhsValue = ((LongLiteralEvaluator) rhs).evaluate(null);
						} else if ( rhs.getEvaluatorType() == STRING_LITERAL ) {
							final SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
							try {
								rhsValue = sdf.parse(((StringLiteralEvaluator) rhs).evaluate(null)).getTime();
							} catch (final Exception e) {
							}
						}
						
						if ( rhsValue != null ) {
							query.add(NumericRangeQuery.newLongRange(fieldName, rhsValue, Long.MAX_VALUE, true, true), Occur.MUST);
						}
					}
				}
				break;
			case LT: {
					final LessThanEvaluator lt = (LessThanEvaluator) whereClause;
					final OperandEvaluator<?> lhs = lt.getLHS();
					final OperandEvaluator<?> rhs = lt.getRHS();
					
					final String fieldName = getFieldName(lhs);
					if ( fieldName != null ) {
						Long rhsValue = null;
						if ( rhs.getEvaluatorType() == NUMBER ) {
							rhsValue = ((LongLiteralEvaluator) rhs).evaluate(null);
						} else if ( rhs.getEvaluatorType() == STRING_LITERAL ) {
							final SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
							try {
								rhsValue = sdf.parse(((StringLiteralEvaluator) rhs).evaluate(null)).getTime();
							} catch (final Exception e) {
							}
						}
						
						if ( rhsValue != null ) {
							query.add(NumericRangeQuery.newLongRange(fieldName, Long.MIN_VALUE, rhsValue, true, true), Occur.MUST);
						}
					}
				}
				break;
			case MATCHES: {
					final MatchesEvaluator me = (MatchesEvaluator) whereClause;
					final OperandEvaluator<?> lhs = me.getLHS();
					final OperandEvaluator<?> rhs = me.getRHS();
					addMatches(lhs, rhs, query);
				}
				break;
			case STARTS_WITH: {
					final StartsWithEvaluator startsWith = (StartsWithEvaluator) whereClause;
					final OperandEvaluator<?> lhs = startsWith.getLHS();
					final OperandEvaluator<?> rhs = startsWith.getRHS();
					
					if ( rhs.getEvaluatorType() == STRING_LITERAL ) {
						final String base = rhs.evaluate(null).toString();
						
						final StringLiteralEvaluator regexEval = new StringLiteralEvaluator(Pattern.quote(base) + ".*");
						addMatches(lhs, regexEval, query);
					}
				}
				break;
			case EQUALS: {
					final EqualsEvaluator equals = (EqualsEvaluator) whereClause;
					final OperandEvaluator<?> lhs = equals.getLHS();
					final OperandEvaluator<?> rhs = equals.getRHS();
					
					final String fieldName = getFieldName(lhs);
					if ( fieldName != null && rhs.getEvaluatorType() == STRING_LITERAL ) {
						query.add(new TermQuery(new Term(fieldName, toLower(rhs.evaluate(null).toString()))), Occur.MUST);
					}
				}
				break;
		}
		
		return query;
	}
	
	private static String toLower(final String value) {
		return value == null ? null : value.toLowerCase();
	}
	
	private static String getFieldName(final OperandEvaluator<?> eval) {
		switch (eval.getEvaluatorType()) {
			case TIMESTAMP:
				return SearchableFields.EventTime.getSearchableFieldName();
			case FILESIZE:
				return SearchableFields.FileSize.getSearchableFieldName();
			case ATTRIBUTE:
				return ((AttributeEvaluator) eval).getAttributeNameEvaluator().evaluate(null).toLowerCase();
			case TRANSIT_URI:
				return SearchableFields.TransitURI.getSearchableFieldName();
			case RELATIONSHIP:
				return SearchableFields.Relationship.getSearchableFieldName();
			case TYPE:
				return SearchableFields.EventType.getSearchableFieldName();
			case COMPONENT_ID:
				return SearchableFields.ComponentID.getSearchableFieldName();
			case UUID:
			    return SearchableFields.FlowFileUUID.getSearchableFieldName();
		}
		
		return null;
	}
	
	private static void addMatches(final OperandEvaluator<?> lhs, final OperandEvaluator<?> rhs, final BooleanQuery query) {
		String field = null;
		switch (lhs.getEvaluatorType()) {
			case ATTRIBUTE:
				final AttributeEvaluator attr = (AttributeEvaluator) lhs;
				final OperandEvaluator<?> attrEval = attr.getAttributeNameEvaluator();
				if ( attrEval.getEvaluatorType() == STRING_LITERAL ) {
					field = (String) attrEval.evaluate(null);
				}
				break;
			case COMPONENT_ID:
			case TRANSIT_URI:
			case TYPE:
				field = lhs.evaluate(null).toString();
				break;
		}
		
		String regex = null;
		if ( rhs.getEvaluatorType() == STRING_LITERAL ) {
			regex = rhs.evaluate(null).toString();
		}
		
		if ( field != null && regex != null ) {
			query.add(new RegexpQuery(new Term(field, regex)), Occur.MUST);
		}
	}
	
}
