package org.apache.nifi.pql;

import static org.apache.nifi.pql.ProvenanceQueryParser.*;

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
import org.apache.nifi.pql.evaluation.extraction.ComponentTypeEvaluator;
import org.apache.nifi.pql.evaluation.extraction.DetailsEvaluator;
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
	
	private final Set<String> referencedFields = new HashSet<>();
	
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

		if ( searchableFields == null ) {
		    this.searchableFields = null;
		} else {
		    final Set<SearchableField> addressableFields = new HashSet<>(searchableFields);
		    addressableFields.add(SearchableFields.EventTime);
		    addressableFields.add(SearchableFields.EventType);
		    this.searchableFields = Collections.unmodifiableSet(addressableFields);
		}
		
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
		
		final BooleanEvaluator where = (whereTree == null) ? null : buildConditionEvaluator(whereTree.getChild(0), Clause.WHERE);
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
	
	public Set<String> getReferencedFields() {
	    return Collections.unmodifiableSet(referencedFields);
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
    			return new SumAccumulator(accumulatorIdGenerator++, toLongEvaluator(buildOperandEvaluator(tree.getChild(0), Clause.SELECT), tree), "SUM(" + getLabel(tree.getChild(0)) + ")");
    		case AVG:
    			return new AverageAccumulator(accumulatorIdGenerator++, toLongEvaluator(buildOperandEvaluator(tree.getChild(0), Clause.SELECT), tree), "AVG(" + getLabel(tree.getChild(0)) + ")");
    		case EVENT:
    			return new EventAccumulator(accumulatorIdGenerator++, getLabel(tree), distinct);
    		case IDENTIFIER:
    			return new EventAccumulator(accumulatorIdGenerator++, getLabel(tree.getChild(0)), distinct);
    		case EVENT_PROPERTY:
    		case ATTRIBUTE:
    			return new EventAccumulator(accumulatorIdGenerator++, getLabel(tree.getChild(0)), buildOperandEvaluator(tree, Clause.SELECT), distinct);
    		case YEAR:
    		case DAY:
    		case HOUR:
    		case MINUTE:
    		case SECOND:
    			return new EventAccumulator(accumulatorIdGenerator++, getLabel(tree), buildOperandEvaluator(tree, Clause.SELECT), distinct);
    		case COUNT:
    			if ( "Event".equalsIgnoreCase(tree.getChild(0).getText() ) ) {
    				return new CountAccumulator(accumulatorIdGenerator++, null, "COUNT(" + getLabel(tree.getChild(0)) + ")");
    			}
    			return new CountAccumulator(accumulatorIdGenerator++, buildOperandEvaluator(tree.getChild(0), Clause.SELECT), "COUNT(" + getLabel(tree.getChild(0)) + ")");
    		default:
				throw new UnsupportedOperationException("Haven't implemented accumulators yet for " + tree);
    	}
    }
    
    private String getLabel(final Tree tree) {
        if ( tree.getChildCount() > 0 ) {
            final Tree childTree = tree.getChild(tree.getChildCount() - 1);
            if ( childTree.getType() == AS ) {
                return childTree.getChild(0).getText();
            }
        }
        
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
    
    
    private void ensureSearchable(final SearchableField field, final boolean addToReferencedFields) {
        if ( searchableFields != null && !searchableFields.contains(field) ) {
            throw new ProvenanceQueryLanguageException("Query cannot reference " + field.getFriendlyName() + " because this field is not searchable by the repository");
        }
        if ( addToReferencedFields ) {
            referencedFields.add(field.getSearchableFieldName());
        }
    }
    
    
    private OperandEvaluator<?> buildOperandEvaluator(final Tree tree, final Clause clause) {
        // When events are pulled back from an index, for efficiency purposes, we may want to know which
        // fields to pull back. The fields in the WHERE clause are irrelevant because they are not shown
        // to the user, so no need to pull those back.
        final boolean isReferenceInteresting = clause != Clause.WHERE;
        
    	switch (tree.getType()) {
    		case EVENT_PROPERTY:
    			switch (tree.getChild(0).getType()) {
    				case FILESIZE:
    				    ensureSearchable(SearchableFields.FileSize, isReferenceInteresting);
    					return new SizeEvaluator();
    				case TRANSIT_URI:
                        ensureSearchable(SearchableFields.TransitURI, isReferenceInteresting);
    					return new TransitUriEvaluator();
    				case TIMESTAMP:
    				    ensureSearchable(SearchableFields.EventTime, isReferenceInteresting);
    					return new TimestampEvaluator();
    				case TYPE:
    				    ensureSearchable(SearchableFields.EventType, isReferenceInteresting);
    				    return new TypeEvaluator();
    				case COMPONENT_ID:
    				    ensureSearchable(SearchableFields.ComponentID, isReferenceInteresting);
    					return new ComponentIdEvaluator();
    				case COMPONENT_TYPE:
    				    ensureSearchable(SearchableFields.ComponentType, isReferenceInteresting);
    				    return new ComponentTypeEvaluator();
    				case RELATIONSHIP:
    				    ensureSearchable(SearchableFields.Relationship, isReferenceInteresting);
                        return new RelationshipEvaluator();
    				case UUID:
    				    ensureSearchable(SearchableFields.FlowFileUUID, isReferenceInteresting);
                        return new UuidEvaluator();
    				case DETAILS:
    				    ensureSearchable(SearchableFields.Details, isReferenceInteresting);
    				    return new DetailsEvaluator();
    				default:
    					// TODO: IMPLEMENT
    					throw new UnsupportedOperationException("Haven't implemented extraction of property " + tree.getChild(0).getText());
    			}
    		case ATTRIBUTE:
    		    final String attributeName = tree.getChild(0).getText();
    		    if ( searchableAttributes != null && !searchableAttributes.contains(attributeName) ) {
    		        throw new ProvenanceQueryLanguageException("Query cannot attribute '" + attributeName + "' because this attribute is not searchable by the repository");
    		    }
    		    
    		    if ( isReferenceInteresting ) {
    		        referencedFields.add(attributeName);
    		    }
    		    
    			return new AttributeEvaluator(toStringEvaluator(buildOperandEvaluator(tree.getChild(0), clause), tree));
    		case STRING_LITERAL:
    			return new StringLiteralEvaluator(tree.getText());
    		case NUMBER:
    			return new LongLiteralEvaluator(Long.valueOf(tree.getText()));
            case YEAR:
                return new TimeFieldEvaluator(toLongEvaluator(buildOperandEvaluator(tree.getChild(0), clause), tree), Calendar.YEAR, YEAR);
            case MONTH:
                return new TimeFieldEvaluator(toLongEvaluator(buildOperandEvaluator(tree.getChild(0), clause), tree), Calendar.MONTH, MONTH);
            case DAY:
                return new TimeFieldEvaluator(toLongEvaluator(buildOperandEvaluator(tree.getChild(0), clause), tree), Calendar.DAY_OF_YEAR, DAY);
    		case HOUR:
    			return new TimeFieldEvaluator(toLongEvaluator(buildOperandEvaluator(tree.getChild(0), clause), tree), Calendar.HOUR_OF_DAY, HOUR);
    		case MINUTE:
    			return new TimeFieldEvaluator(toLongEvaluator(buildOperandEvaluator(tree.getChild(0), clause), tree), Calendar.MINUTE, MINUTE);
    		case SECOND:
    			return new TimeFieldEvaluator(toLongEvaluator(buildOperandEvaluator(tree.getChild(0), clause), tree), Calendar.SECOND, SECOND);
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
    
    
    private BooleanEvaluator buildConditionEvaluator(final Tree tree, final Clause clause) {
    	switch (tree.getType()) {
    		case AND:
    			return new AndEvaluator(buildConditionEvaluator(tree.getChild(0), clause), buildConditionEvaluator(tree.getChild(1), clause));
    		case OR:
    			return new OrEvaluator(buildConditionEvaluator(tree.getChild(0), clause), buildConditionEvaluator(tree.getChild(1), clause));
    		case EQUALS:
    			return new EqualsEvaluator(buildOperandEvaluator(tree.getChild(0), clause), buildOperandEvaluator(tree.getChild(1), clause));
    		case NOT_EQUALS:
    			return new EqualsEvaluator(buildOperandEvaluator(tree.getChild(0), clause), buildOperandEvaluator(tree.getChild(1), clause), true);
    		case GT:
    			return new GreaterThanEvaluator(buildOperandEvaluator(tree.getChild(0), clause), buildOperandEvaluator(tree.getChild(1), clause));
    		case LT:
    			return new LessThanEvaluator(buildOperandEvaluator(tree.getChild(0), clause), buildOperandEvaluator(tree.getChild(1), clause));
    		case NOT:
    			return buildConditionEvaluator(tree.getChild(0), clause).negate();
    		case MATCHES: {
    			final OperandEvaluator<?> rhs = buildOperandEvaluator(tree.getChild(1), clause);
    			if ( !String.class.equals( rhs.getType() ) ) {
    				throw new ProvenanceQueryLanguageParsingException("Right-hand side of MATCHES operator must be a Regular Expression but found " + rhs);
    			}
    			return new MatchesEvaluator(buildOperandEvaluator(tree.getChild(0), clause), rhs);
    		}
    		case STARTS_WITH: {
    			final OperandEvaluator<?> rhs = buildOperandEvaluator(tree.getChild(1), clause);
    			if ( !String.class.equals( rhs.getType() ) ) {
    				throw new ProvenanceQueryLanguageParsingException("Right-hand side of STARTS WITH operator must be a String but found " + rhs);
    			}
    			return new StartsWithEvaluator(buildOperandEvaluator(tree.getChild(0), clause), rhs);
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
    				evaluator = buildOperandEvaluator(tree, Clause.GROUP);
    				break;
    			default:
    				evaluator = buildConditionEvaluator(tree, Clause.GROUP);
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
	    		final OperandEvaluator<?> evaluator = buildOperandEvaluator(orderTree.getChild(0), Clause.ORDER);
	    		
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
    
    private static enum Clause {
        SELECT,
        FROM,
        WHERE,
        GROUP,
        ORDER;
    }
}
