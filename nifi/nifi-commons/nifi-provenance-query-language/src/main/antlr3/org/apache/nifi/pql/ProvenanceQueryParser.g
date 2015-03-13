parser grammar ProvenanceQueryParser;

options {
	output=AST;
	tokenVocab=ProvenanceQueryLexer;
}

tokens {
	PQL;
	QUERY;
	EVENT_PROPERTY;
	ATTRIBUTE;
	ORDER;
}

@header {
	package org.apache.nifi.pql;
	import org.apache.nifi.pql.exception.ProvenanceQueryLanguageParsingException;
}


@members {
  public void displayRecognitionError(String[] tokenNames, RecognitionException e) {
  	final StringBuilder sb = new StringBuilder();
    if ( e.token == null ) {
    	sb.append("Unrecognized token ");
    } else {
    	sb.append("Unexpected token '").append(e.token.getText()).append("' ");
    }
    sb.append("at line ").append(e.line);
    if ( e.approximateLineInfo ) {
    	sb.append(" (approximately)");
    }
    sb.append(", column ").append(e.charPositionInLine);
    sb.append(". Query: ").append(e.input.toString());
    
    throw new ProvenanceQueryLanguageParsingException(sb.toString());
  }

  public void recover(final RecognitionException e) {
  	final StringBuilder sb = new StringBuilder();
    if ( e.token == null ) {
    	sb.append("Unrecognized token ");
    } else {
    	sb.append("Unexpected token '").append(e.token.getText()).append("' ");
    }
    sb.append("at line ").append(e.line);
    if ( e.approximateLineInfo ) {
    	sb.append(" (approximately)");
    }
    sb.append(", column ").append(e.charPositionInLine);
    sb.append(". Query: ").append(e.input.toString());
    
    throw new ProvenanceQueryLanguageParsingException(sb.toString());
  } 
}



pql : query ->
	^(PQL query);

query : selectClause
		fromClause?
		whereClause?
		groupByClause?
//		havingClause?
		orderByClause?
		limitClause?
		SEMICOLON?
		EOF ->
	^(QUERY selectClause fromClause? whereClause? groupByClause? orderByClause? limitClause?);


selectClause : SELECT^ selectable (COMMA! selectable)*;


selectable : (function^ | (selectableSource ( (DOT! eventProperty^) | (LBRACKET! attribute^ RBRACKET!) )?)^) (AS IDENTIFIER)?;

selectableSource : EVENT | IDENTIFIER;

eventProperty : propertyName ->
	^(EVENT_PROPERTY propertyName );

propertyName : UUID | TRANSIT_URI | TIMESTAMP | FILESIZE | TYPE | COMPONENT_ID | COMPONENT_TYPE | RELATIONSHIP;

attribute : STRING_LITERAL ->
	^(ATTRIBUTE STRING_LITERAL);


fromClause : FROM^ source (COMMA! source)*;

source : RECEIVE | SEND | DROP | CREATE | EXPIRE | FORK | JOIN | CLONE | CONTENT_MODIFIED | ATTRIBUTES_MODIFIED | ROUTE | REPLAY | ASTERISK;



whereClause : WHERE^ conditions;

conditions : condition ((AND^ | OR^) condition)*;

condition : NOT^ condition | LPAREN! conditions RPAREN! | evaluation;

evaluation : expression 
			(
				unaryOperator^ 
				| (binaryOperator^ expression)
				| (BETWEEN^ NUMBER AND! NUMBER)
			);

expression : (LPAREN! expr RPAREN!) | expr;

expr : constant | ref;

ref : selectableSource ( (DOT! eventProperty^) | (LBRACKET! attribute^ RBRACKET!) )?;

unaryOperator : IS_NULL | NOT_NULL;

binaryOperator : EQUALS | NOT_EQUALS | GT | LT | GE | LE | MATCHES | STARTS_WITH;

constant : NUMBER | STRING_LITERAL;


function : functionName^ LPAREN! ref RPAREN!;

functionName : COUNT | SUM | MIN | MAX | AVG | HOUR | MINUTE | SECOND | DAY | MONTH | YEAR;


groupByClause : GROUP_BY^ group (COMMA! group)*;

group : ref^ | function^;

orderByClause : ORDER_BY^ order (COMMA! order)*;

order: selectable direction? ->
	^(ORDER selectable direction?);

direction : ASC | DESC;

limitClause : LIMIT^ NUMBER;


