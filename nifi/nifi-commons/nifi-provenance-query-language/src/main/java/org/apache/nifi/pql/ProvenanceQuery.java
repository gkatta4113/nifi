package org.apache.nifi.pql;

import static org.apache.nifi.pql.ProvenanceQueryParser.AND;
import static org.apache.nifi.pql.ProvenanceQueryParser.ASC;
import static org.apache.nifi.pql.ProvenanceQueryParser.ATTRIBUTE;
import static org.apache.nifi.pql.ProvenanceQueryParser.AVG;
import static org.apache.nifi.pql.ProvenanceQueryParser.COMPONENT_ID;
import static org.apache.nifi.pql.ProvenanceQueryParser.COUNT;
import static org.apache.nifi.pql.ProvenanceQueryParser.DAY;
import static org.apache.nifi.pql.ProvenanceQueryParser.EQUALS;
import static org.apache.nifi.pql.ProvenanceQueryParser.EVENT;
import static org.apache.nifi.pql.ProvenanceQueryParser.EVENT_PROPERTY;
import static org.apache.nifi.pql.ProvenanceQueryParser.FILESIZE;
import static org.apache.nifi.pql.ProvenanceQueryParser.FROM;
import static org.apache.nifi.pql.ProvenanceQueryParser.GROUP_BY;
import static org.apache.nifi.pql.ProvenanceQueryParser.GT;
import static org.apache.nifi.pql.ProvenanceQueryParser.HOUR;
import static org.apache.nifi.pql.ProvenanceQueryParser.IDENTIFIER;
import static org.apache.nifi.pql.ProvenanceQueryParser.LIMIT;
import static org.apache.nifi.pql.ProvenanceQueryParser.LT;
import static org.apache.nifi.pql.ProvenanceQueryParser.MATCHES;
import static org.apache.nifi.pql.ProvenanceQueryParser.MINUTE;
import static org.apache.nifi.pql.ProvenanceQueryParser.MONTH;
import static org.apache.nifi.pql.ProvenanceQueryParser.NOT;
import static org.apache.nifi.pql.ProvenanceQueryParser.NOT_EQUALS;
import static org.apache.nifi.pql.ProvenanceQueryParser.NUMBER;
import static org.apache.nifi.pql.ProvenanceQueryParser.OR;
import static org.apache.nifi.pql.ProvenanceQueryParser.ORDER_BY;
import static org.apache.nifi.pql.ProvenanceQueryParser.RELATIONSHIP;
import static org.apache.nifi.pql.ProvenanceQueryParser.SECOND;
import static org.apache.nifi.pql.ProvenanceQueryParser.STARTS_WITH;
import static org.apache.nifi.pql.ProvenanceQueryParser.STRING_LITERAL;
import static org.apache.nifi.pql.ProvenanceQueryParser.SUM;
import static org.apache.nifi.pql.ProvenanceQueryParser.TIMESTAMP;
import static org.apache.nifi.pql.ProvenanceQueryParser.TRANSIT_URI;
import static org.apache.nifi.pql.ProvenanceQueryParser.TYPE;
import static org.apache.nifi.pql.ProvenanceQueryParser.UUID;
import static org.apache.nifi.pql.ProvenanceQueryParser.WHERE;
import static org.apache.nifi.pql.ProvenanceQueryParser.YEAR;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.tree.Tree;
import org.apache.nifi.pql.evaluation.Accumulator;
import org.apache.nifi.pql.evaluation.BooleanEvaluator;
import org.apache.nifi.pql.evaluation.OperandEvaluator;
import org.apache.nifi.pql.evaluation.RecordEvaluator;
import org.apache.nifi.pql.evaluation.RepositoryEvaluator;
import org.apache.nifi.pql.evaluation.accumulation.AverageAccumulator;
import org.apache.nifi.pql.evaluation.accumulation.CountAccumulator;
import org.apache.nifi.pql.evaluation.accumulation.EventAccumulator;
import org.apache.nifi.pql.evaluation.accumulation.SumAccumulator;
import org.apache.nifi.pql.evaluation.comparison.EqualsEvaluator;
import org.apache.nifi.pql.evaluation.comparison.GreaterThanEvaluator;
import org.apache.nifi.pql.evaluation.comparison.LessThanEvaluator;
import org.apache.nifi.pql.evaluation.comparison.MatchesEvaluator;
import org.apache.nifi.pql.evaluation.comparison.RecordTypeEvaluator;
import org.apache.nifi.pql.evaluation.comparison.StartsWithEvaluator;
import org.apache.nifi.pql.evaluation.conversion.StringToLongEvaluator;
import org.apache.nifi.pql.evaluation.extraction.AttributeEvaluator;
import org.apache.nifi.pql.evaluation.extraction.ComponentIdEvaluator;
import org.apache.nifi.pql.evaluation.extraction.RelationshipEvaluator;
import org.apache.nifi.pql.evaluation.extraction.SizeEvaluator;
import org.apache.nifi.pql.evaluation.extraction.TimestampEvaluator;
import org.apache.nifi.pql.evaluation.extraction.TransitUriEvaluator;
import org.apache.nifi.pql.evaluation.extraction.TypeEvaluator;
import org.apache.nifi.pql.evaluation.extraction.UuidEvaluator;
import org.apache.nifi.pql.evaluation.function.TimeFieldEvaluator;
import org.apache.nifi.pql.evaluation.literals.LongLiteralEvaluator;
import org.apache.nifi.pql.evaluation.literals.StringLiteralEvaluator;
import org.apache.nifi.pql.evaluation.logic.AndEvaluator;
import org.apache.nifi.pql.evaluation.logic.OrEvaluator;
import org.apache.nifi.pql.evaluation.order.FieldSorter;
import org.apache.nifi.pql.evaluation.order.GroupedSorter;
import org.apache.nifi.pql.evaluation.order.RowSorter;
import org.apache.nifi.pql.evaluation.order.SortDirection;
import org.apache.nifi.pql.evaluation.repository.SelectAllRecords;
import org.apache.nifi.pql.exception.ProvenanceQueryLanguageException;
import org.apache.nifi.pql.exception.ProvenanceQueryLanguageParsingException;
import org.apache.nifi.pql.results.GroupingResultSet;
import org.apache.nifi.pql.results.StandardOrderedResultSet;
import org.apache.nifi.pql.results.StandardUnorderedResultSet;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.provenance.SearchableFields;
import org.apache.nifi.provenance.StoredProvenanceEvent;
import org.apache.nifi.provenance.query.ProvenanceResultSet;
import org.apache.nifi.provenance.search.SearchableField;

public class ProvenanceQuery {
	private final Tree tree;
	private final String pql;
	private final List<Accumulator<?>> selectAccumulators;
	private final List<RecordEvaluator<?>> groupEvaluators;
	private final RecordEvaluator<Boolean> sourceEvaluator;
	private final RecordEvaluator<Boolean> conditionEvaluator;
	private final RowSorter sorter;
	private final Long limit;
	
	private final Set<SearchableField> searchableFields;
	private final Set<String> searchableAttributes;
	private long accumulatorIdGenerator = 0L;
	
	
	public static ProvenanceQuery compile(final String pql, final Collection<SearchableField> searchableFields, final Collection<SearchableField> searchableAttributes) {
		try {
            final CommonTokenStream lexerTokenStream = createTokenStream(pql);
            final ProvenanceQueryParser parser = new ProvenanceQueryParser(lexerTokenStream);
            final Tree ast = (Tree) parser.pql().getTree();
            final Tree tree = ast.getChild(0);
            
            return new ProvenanceQuery(tree, pql, searchableFields, searchableAttributes);
        } catch (final ProvenanceQueryLanguageParsingException e) {
            throw e;
        } catch (final Exception e) {
            throw new ProvenanceQueryLanguageParsingException(e);
        }
	}
	
	private static CommonTokenStream createTokenStream(final String expression) throws ProvenanceQueryLanguageParsingException {
        final CharStream input = new ANTLRStringStream(expression);
        final ProvenanceQueryLexer lexer = new ProvenanceQueryLexer(input);
        return new CommonTokenStream(lexer);
    }
	
	private ProvenanceQuery(final Tree tree, final String pql, final Collection<SearchableField> searchableFields, final Collection<SearchableField> searchableAttributes) {
		this.tree = tree;
		this.pql = pql;
		this.searchableFields = searchableFields == null ? null : Collections.unmodifiableSet(new HashSet<>(searchableFields));
		if (searchableAttributes == null) {
		    this.searchableAttributes = null;
		} else {
    		final Set<String> attributes = new HashSet<>();
    		for ( final SearchableField attr : searchableAttributes ) {
    		    attributes.add(attr.getSearchableFieldName());
    		}
    		this.searchableAttributes = Collections.unmodifiableSet(attributes);
		}
		
		Tree fromTree = null;
		Tree whereTree = null;
		Tree groupByTree = null;
		Tree limitTree = null;
		Tree orderByTree = null;
		
		for (int i=1; i < tree.getChildCount(); i++) {
			final Tree subTree = tree.getChild(i);
			switch (subTree.getType()) {
				case FROM:
					fromTree = subTree;
					break;
				case WHERE:
					whereTree = subTree;
					break;
				case GROUP_BY:
					groupByTree = subTree;
					break;
				case LIMIT:	
					limitTree = subTree;
					break;
				case ORDER_BY:
					orderByTree = subTree;
					break;
				default:
					// TODO: Handle other types!
					continue;
			}
		}

		sourceEvaluator = (fromTree == null) ? null : buildSourceEvaluator(fromTree);
		
		final BooleanEvaluator where = (whereTree == null) ? null : buildConditionEvaluator(whereTree.getChild(0));
		conditionEvaluator = where;
		
		groupEvaluators = (groupByTree == null) ? null : buildGroupEvaluators(groupByTree);
		limit = (limitTree == null) ? null : Long.parseLong(limitTree.getChild(0).getText());
		sorter = (orderByTree == null) ? null : buildSorter(orderByTree, groupByTree != null);
		
		boolean requiresAggregate = false;
		if ( groupEvaluators != null && !groupEvaluators.isEmpty() ) {
			requiresAggregate = true;
		}
		if ( requiresAggregate ) {
			selectAccumulators = buildAccumulators(tree.getChild(0), true);
		} else {
			final List<Accumulator<?>> accumulators = buildAccumulators(tree.getChild(0), false);
			
			for ( final Accumulator<?> accumulator : accumulators ) {
				if ( accumulator.isAggregateFunction() ) {
					requiresAggregate = true;
					break;
				}
			}
			
			if ( requiresAggregate ) {
				selectAccumulators = buildAccumulators(tree.getChild(0), true);
			} else {
				selectAccumulators = accumulators;
			}
		}
	}
	
	@Override
	public String toString() {
		return printTree(tree);
	}
	
	public String getQuery() {
		return pql;
		
	}
	
	private String printTree(final Tree tree) {
        final StringBuilder sb = new StringBuilder();
        printTree(tree, 0, sb);
        
        return sb.toString();
    }
    
    private void printTree(final Tree tree, final int spaces, final StringBuilder sb) {
        for (int i=0; i < spaces; i++) {
            sb.append(" ");
        }
        
        if ( tree.getText().trim().isEmpty() ) {
            sb.append(tree.toString()).append("\n");
        } else {
            sb.append(tree.getText()).append("\n");
        }
        
        for (int i=0; i < tree.getChildCount(); i++) {
            printTree(tree.getChild(i), spaces + 2, sb);
        }
    }
    
    private List<Accumulator<?>> buildAccumulators(final Tree selectTree, final boolean distinct) {
    	final List<Accumulator<?>> accumulators = new ArrayList<>();
    	
    	if ( selectTree.getType() != ProvenanceQueryParser.SELECT ) {
    		throw new IllegalArgumentException("Cannot build accumulators for a non-SELECT tree");
    	}
    	
    	for (int i=0; i < selectTree.getChildCount(); i++) {
    		final Tree childTree = selectTree.getChild(i);
    		accumulators.add(buildAccumulator(childTree, distinct));
    	}
    	
    	return accumulators;
    }
    
    private Accumulator<?> buildAccumulator(final Tree tree, final boolean distinct) {
    	switch (tree.getType()) {
    		case SUM:
    			return new SumAccumulator(accumulatorIdGenerator++, toLongEvaluator(buildOperandEvaluator(tree.getChild(0)), tree), "SUM(" + getLabel(tree.getChild(0)) + ")");
    		case AVG:
    			return new AverageAccumulator(accumulatorIdGenerator++, toLongEvaluator(buildOperandEvaluator(tree.getChild(0)), tree), "AVG(" + getLabel(tree.getChild(0)) + ")");
    		case EVENT:
    			return new EventAccumulator(accumulatorIdGenerator++, getLabel(tree), distinct);
    		case IDENTIFIER:
    			return new EventAccumulator(accumulatorIdGenerator++, getLabel(tree.getChild(0)), distinct);
    		case EVENT_PROPERTY:
    		case ATTRIBUTE:
    			return new EventAccumulator(accumulatorIdGenerator++, getLabel(tree.getChild(0)), buildOperandEvaluator(tree), distinct);
    		case YEAR:
    		case DAY:
    		case HOUR:
    		case MINUTE:
    		case SECOND:
    			return new EventAccumulator(accumulatorIdGenerator++, getLabel(tree), buildOperandEvaluator(tree), distinct);
    		case COUNT:
    			if ( "Event".equalsIgnoreCase(tree.getChild(0).getText() ) ) {
    				return new CountAccumulator(accumulatorIdGenerator++, null, "COUNT(" + getLabel(tree.getChild(0)) + ")");
    			}
    			return new CountAccumulator(accumulatorIdGenerator++, buildOperandEvaluator(tree.getChild(0)), "COUNT(" + getLabel(tree.getChild(0)) + ")");
    		default:
				throw new UnsupportedOperationException("Haven't implemented accumulators yet for " + tree);
    	}
    }
    
    private String getLabel(final Tree tree) {
    	final int type = tree.getType();
    	
    	switch (type) {
    		case EVENT_PROPERTY:
    		case ATTRIBUTE:
    			return tree.getChild(0).getText();
    		case YEAR:
    		case DAY:
    		case HOUR:
    		case MINUTE:
    		case SECOND:
    			return tree.getText() + "(" + getLabel(tree.getChild(0)) + ")";
    	}
    	
    	return tree.getText();
    }
    
    private OperandEvaluator<?> buildOperandEvaluator(final Tree tree) {
    	switch (tree.getType()) {
    		case EVENT_PROPERTY:
    			switch (tree.getChild(0).getType()) {
    				case FILESIZE:
    				    if ( searchableFields != null && !searchableFields.contains(SearchableFields.FileSize) ) {
    				        throw new ProvenanceQueryLanguageException("Query cannot reference FileSize because this field is not searchable by the repository");
    				    }
    					return new SizeEvaluator();
    				case TRANSIT_URI:
    				    if ( searchableFields != null && !searchableFields.contains(SearchableFields.TransitURI) ) {
                            throw new ProvenanceQueryLanguageException("Query cannot reference TransitURI because this field is not searchable by the repository");
                        }
    					return new TransitUriEvaluator();
    				case TIMESTAMP:
    				    // time is always indexed
    					return new TimestampEvaluator();
    				case TYPE:
    				    // type is always indexed so no need to check it
    					return new TypeEvaluator();
    				case COMPONENT_ID:
    				    if ( searchableFields != null && !searchableFields.contains(SearchableFields.ComponentID) ) {
                            throw new ProvenanceQueryLanguageException("Query cannot reference Component ID because this field is not searchable by the repository");
                        }
    					return new ComponentIdEvaluator();
    					// TODO: Allow Component Type to be indexed and searched
    				case RELATIONSHIP:
    				    if ( searchableFields != null && !searchableFields.contains(SearchableFields.Relationship) ) {
                            throw new ProvenanceQueryLanguageException("Query cannot reference Relationship because this field is not searchable by the repository");
                        }
    					return new RelationshipEvaluator();
    				case UUID:
    				    if ( searchableFields != null && !searchableFields.contains(SearchableFields.FlowFileUUID) ) {
                            throw new ProvenanceQueryLanguageException("Query cannot reference FlowFile UUID because this field is not searchable by the repository");
                        }
    				    return new UuidEvaluator();
    				default:
    					// TODO: IMPLEMENT
    					throw new UnsupportedOperationException("Haven't implemented extraction of property " + tree.getChild(0).getText());
    			}
    		case ATTRIBUTE:
    		    final String attributeName = tree.getChild(0).getText();
    		    if ( searchableAttributes != null && !searchableAttributes.contains(attributeName) ) {
    		        throw new ProvenanceQueryLanguageException("Query cannot attribute '" + attributeName + "' because this attribute is not searchable by the repository");
    		    }
    			return new AttributeEvaluator(toStringEvaluator(buildOperandEvaluator(tree.getChild(0)), tree));
    		case STRING_LITERAL:
    			return new StringLiteralEvaluator(tree.getText());
    		case NUMBER:
    			return new LongLiteralEvaluator(Long.valueOf(tree.getText()));
            case YEAR:
                return new TimeFieldEvaluator(toLongEvaluator(buildOperandEvaluator(tree.getChild(0)), tree), Calendar.YEAR, YEAR);
            case MONTH:
                return new TimeFieldEvaluator(toLongEvaluator(buildOperandEvaluator(tree.getChild(0)), tree), Calendar.MONTH, MONTH);
            case DAY:
                return new TimeFieldEvaluator(toLongEvaluator(buildOperandEvaluator(tree.getChild(0)), tree), Calendar.DAY_OF_YEAR, DAY);
    		case HOUR:
    			return new TimeFieldEvaluator(toLongEvaluator(buildOperandEvaluator(tree.getChild(0)), tree), Calendar.HOUR_OF_DAY, HOUR);
    		case MINUTE:
    			return new TimeFieldEvaluator(toLongEvaluator(buildOperandEvaluator(tree.getChild(0)), tree), Calendar.MINUTE, MINUTE);
    		case SECOND:
    			return new TimeFieldEvaluator(toLongEvaluator(buildOperandEvaluator(tree.getChild(0)), tree), Calendar.SECOND, SECOND);
    		default:
    			throw new ProvenanceQueryLanguageParsingException("Unable to extract value '" + tree.toString() + "' from event because it is not a valid ");
    	}
    }
    
    
    private RecordEvaluator<Boolean> buildSourceEvaluator(final Tree fromTree) {
    	if ( fromTree == null ) {
    		throw new NullPointerException();
    	}
    	if ( fromTree.getType() != FROM ) {
    		throw new IllegalArgumentException("Cannot build Soruce Evaluator from a Tree that is not a FROM-tree");
    	}
    	
    	final Set<ProvenanceEventType> types = new HashSet<>();
    	for ( int i=0; i < fromTree.getChildCount(); i++ ) {
    		final Tree typeTree = fromTree.getChild(i);
    		if ( "*".equals(typeTree.getText()) ) {
    			return null;
    		} else {
    			types.add(ProvenanceEventType.valueOf(typeTree.getText().toUpperCase()));
    		}
    	}
    	
    	return new RecordTypeEvaluator(types);
    }
    
    
    private BooleanEvaluator buildConditionEvaluator(final Tree tree) {
    	switch (tree.getType()) {
    		case AND:
    			return new AndEvaluator(buildConditionEvaluator(tree.getChild(0)), buildConditionEvaluator(tree.getChild(1)));
    		case OR:
    			return new OrEvaluator(buildConditionEvaluator(tree.getChild(0)), buildConditionEvaluator(tree.getChild(1)));
    		case EQUALS:
    			return new EqualsEvaluator(buildOperandEvaluator(tree.getChild(0)), buildOperandEvaluator(tree.getChild(1)));
    		case NOT_EQUALS:
    			return new EqualsEvaluator(buildOperandEvaluator(tree.getChild(0)), buildOperandEvaluator(tree.getChild(1)), true);
    		case GT:
    			return new GreaterThanEvaluator(buildOperandEvaluator(tree.getChild(0)), buildOperandEvaluator(tree.getChild(1)));
    		case LT:
    			return new LessThanEvaluator(buildOperandEvaluator(tree.getChild(0)), buildOperandEvaluator(tree.getChild(1)));
    		case NOT:
    			return buildConditionEvaluator(tree.getChild(0)).negate();
    		case MATCHES: {
    			final OperandEvaluator<?> rhs = buildOperandEvaluator(tree.getChild(1));
    			if ( !String.class.equals( rhs.getType() ) ) {
    				throw new ProvenanceQueryLanguageParsingException("Right-hand side of MATCHES operator must be a Regular Expression but found " + rhs);
    			}
    			return new MatchesEvaluator(buildOperandEvaluator(tree.getChild(0)), rhs);
    		}
    		case STARTS_WITH: {
    			final OperandEvaluator<?> rhs = buildOperandEvaluator(tree.getChild(1));
    			if ( !String.class.equals( rhs.getType() ) ) {
    				throw new ProvenanceQueryLanguageParsingException("Right-hand side of STARTS WITH operator must be a String but found " + rhs);
    			}
    			return new StartsWithEvaluator(buildOperandEvaluator(tree.getChild(0)), rhs);
    		}
    		default:
    			// TODO: Implement
    			throw new UnsupportedOperationException("Have not yet implemented condition evaluator for " + tree);
    	}
    }
    
    
    private <T> OperandEvaluator<T> castEvaluator(final OperandEvaluator<?> eval, final Tree tree, final Class<T> expectedType) {
    	if ( eval.getType() != expectedType ) {
    		throw new ProvenanceQueryLanguageParsingException("Expected type " + expectedType.getSimpleName() + " but found type " + eval.getType() + " for term: " + tree);
    	}

    	@SuppressWarnings("unchecked")
		final OperandEvaluator<T> retEvaluator = ((OperandEvaluator<T>) eval);
    	return retEvaluator;

    }
    
    private OperandEvaluator<String> toStringEvaluator(final OperandEvaluator<?> eval, final Tree tree) {
    	return castEvaluator(eval, tree, String.class);
    }
    
    private OperandEvaluator<Long> toLongEvaluator(final OperandEvaluator<?> eval, final Tree tree) {
        if ( eval.getType() == Long.class ) {
            @SuppressWarnings("unchecked")
            final OperandEvaluator<Long> retEvaluator = ((OperandEvaluator<Long>) eval);
            return retEvaluator;
        } else if ( eval.getType() == String.class ) {
            @SuppressWarnings("unchecked")
            final OperandEvaluator<String> stringEval = ((OperandEvaluator<String>) eval);
            return new StringToLongEvaluator(stringEval);
        }
        
    	return castEvaluator(eval, tree, Long.class);
    }
    
    
    private List<RecordEvaluator<?>> buildGroupEvaluators(final Tree groupByTree) {
    	if ( groupByTree == null ) {
    		return null;
    	}

    	if ( groupByTree.getType() != GROUP_BY ) {
    		throw new IllegalArgumentException("Expected GroupBy tree but got " + groupByTree);
    	}
    	
    	final List<RecordEvaluator<?>> evaluators = new ArrayList<>(groupByTree.getChildCount());
    	for (int i=0; i < groupByTree.getChildCount(); i++) {
    		final Tree tree = groupByTree.getChild(i);
    		final RecordEvaluator<?> evaluator;
    		
    		switch (tree.getType()) {
    			case EVENT_PROPERTY:
    			case STRING_LITERAL:
    			case ATTRIBUTE:
    			case YEAR:
    			case DAY:
    			case HOUR:
    			case MINUTE:
    			case SECOND:
    				evaluator = buildOperandEvaluator(tree);
    				break;
    			default:
    				evaluator = buildConditionEvaluator(tree);
    				break;
    		}
    		
    		evaluators.add(evaluator);
    	}
    	
    	return evaluators;
    }
    
    private RowSorter buildSorter(final Tree orderByTree, final boolean grouped) {
    	if ( orderByTree.getType() != ORDER_BY ) {
    		throw new IllegalArgumentException();
    	}
    	
    	if ( grouped ) {
    		final Map<Accumulator<?>, SortDirection> accumulators = new LinkedHashMap<>(orderByTree.getChildCount());
    		for (int i=0; i < orderByTree.getChildCount(); i++) {
    			final Tree orderTree = orderByTree.getChild(i);
    			final Accumulator<?> accumulator = buildAccumulator(orderTree.getChild(0), true);

	    		final SortDirection sortDir;
	    		if ( orderTree.getChildCount() > 1 ) {
	    			final int sortDirType = orderTree.getChild(1).getType();
	    			sortDir = (sortDirType == ASC) ? SortDirection.ASC : SortDirection.DESC;
	    		} else {
	    			sortDir = SortDirection.ASC;
	    		}

	    		accumulators.put(accumulator, sortDir);
    		}
    		
    		return new GroupedSorter(accumulators);
    	} else {
	    	// TODO: Allow ORDER BY of aggregate values
	    	final Map<OperandEvaluator<?>, SortDirection> evaluators = new LinkedHashMap<>(orderByTree.getChildCount());
	    	for (int i=0; i < orderByTree.getChildCount(); i++) {
	    		final Tree orderTree = orderByTree.getChild(i);
	    		final OperandEvaluator<?> evaluator = buildOperandEvaluator(orderTree.getChild(0));
	    		
	    		final SortDirection sortDir;
	    		if ( orderTree.getChildCount() > 1 ) {
	    			final int sortDirType = orderTree.getChild(1).getType();
	    			sortDir = (sortDirType == ASC) ? SortDirection.ASC : SortDirection.DESC;
	    		} else {
	    			sortDir = SortDirection.ASC;
	    		}
	    		
	    		evaluators.put(evaluator, sortDir);
	    	}
	
	    	return new FieldSorter(evaluators);
    	}
    }
    
    
    public static ProvenanceResultSet execute(final String query, final ProvenanceEventRepository repo) throws IOException {
    	return ProvenanceQuery.compile(query, null, null).execute(repo);
    }
    
    
    public ProvenanceResultSet evaluate(final Iterator<? extends StoredProvenanceEvent> matchedEvents) {
        final List<String> labels = new ArrayList<>();
        final List<Class<?>> returnTypes = new ArrayList<>(selectAccumulators.size());
        
        for ( final Accumulator<?> accumulator : selectAccumulators ) {
            labels.add(accumulator.getLabel());
            returnTypes.add(accumulator.getReturnType());
        }
        
        ProvenanceResultSet rs;
        if ( isAggregateRequired() ) {
            rs = new GroupingResultSet(matchedEvents, 
                selectAccumulators, sourceEvaluator, conditionEvaluator,
                labels, returnTypes, groupEvaluators, sorter, limit);
        } else if (sorter == null) {
            rs = new StandardUnorderedResultSet(matchedEvents, selectAccumulators, sourceEvaluator, conditionEvaluator, labels, returnTypes, limit);
        } else {
            rs = new StandardOrderedResultSet(matchedEvents, selectAccumulators, sourceEvaluator, conditionEvaluator, labels, returnTypes, sorter, limit);
        }
        
        return rs;
    }
    
    public ProvenanceResultSet execute(final ProvenanceEventRepository repo) throws IOException {
    	final RepositoryEvaluator repoEvaluator = new SelectAllRecords();
    	
    	final Iterator<StoredProvenanceEvent> potentialMatches = repoEvaluator.evaluate(repo);
    	final List<String> labels = new ArrayList<>();
    	final List<Class<?>> returnTypes = new ArrayList<>(selectAccumulators.size());
    	
		for ( final Accumulator<?> accumulator : selectAccumulators ) {
			labels.add(accumulator.getLabel());
			returnTypes.add(accumulator.getReturnType());
		}
		
		ProvenanceResultSet rs;
		if ( isAggregateRequired() ) {
			rs = new GroupingResultSet(potentialMatches, 
				selectAccumulators, sourceEvaluator, conditionEvaluator,
				labels, returnTypes, groupEvaluators, sorter, limit);
		} else if (sorter == null) {
			rs = new StandardUnorderedResultSet(potentialMatches, selectAccumulators, sourceEvaluator, conditionEvaluator, labels, returnTypes, limit);
		} else {
			rs = new StandardOrderedResultSet(potentialMatches, selectAccumulators, sourceEvaluator, conditionEvaluator, labels, returnTypes, sorter, limit);
		}
		
		return rs;
    }
    
    private boolean isAggregateRequired() {
		if ( groupEvaluators != null && !groupEvaluators.isEmpty() ) {
			return true;
		}
		
		for ( final Accumulator<?> accumulator : selectAccumulators ) {
			if ( accumulator.isAggregateFunction() ) {
				return true;
			}
		}
		
		
		return false;
    }
    
    
    public RecordEvaluator<Boolean> getWhereClause() {
    	return conditionEvaluator;
    }
}
