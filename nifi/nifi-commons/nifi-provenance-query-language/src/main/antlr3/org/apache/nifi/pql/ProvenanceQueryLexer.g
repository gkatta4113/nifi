lexer grammar ProvenanceQueryLexer;

@header {
	package org.apache.nifi.pql;
	import org.apache.nifi.pql.exception.ProvenanceQueryLanguageParsingException;
}

@rulecatch {
  catch(final Exception e) {
    throw new ProvenanceQueryLanguageParsingException(e);
  }
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

  public void recover(RecognitionException e) {
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


// PUNCTUATION & SPECIAL CHARACTERS
WHITESPACE : (' '|'\t'|'\n'|'\r')+ { $channel = HIDDEN; };
COMMENT : '#' ( ~('\n') )* '\n' { $channel = HIDDEN; };

LPAREN	: '(';
RPAREN	: ')';
LBRACKET : '[';
RBRACKET : ']';
COMMA	: ',';
DOT		: '.';
SEMICOLON : ';';
ASTERISK : '*';
EQUALS	: '=';
NOT_EQUALS : '!=' | '<>';
GT : '>';
LT : '<';
GE : '>=';
LE : '<=';
PIPE : '|';
NUMBER	: ('0'..'9')+;


// Keywords
SELECT : 'SELECT' | 'select' | 'Select';
AS : 'AS' | 'as' | 'As';
FROM : 'FROM' | 'from' | 'From';
WHERE : 'WHERE' | 'where' | 'Where';
HAVING : 'HAVING' | 'having' | 'Having';
ORDER_BY : 'ORDER BY' | 'order by' | 'Order By';
ASC : 'ASC' | 'asc' | 'Asc';
DESC : 'DESC' | 'desc' | 'Desc';
GROUP_BY : 'GROUP BY' | 'group by' | 'Group By';
EVENT : 'EVENT' | 'event' | 'Event';
RELATIONSHIP : 'RELATIONSHIP' | 'relationship' | 'Relationship';


// Operators
WITHIN : 'WITHIN' | 'within' | 'Within';
MATCHES : 'MATCHES' | 'matches' | 'Matches';
CONTAINS : 'CONTAINS' | 'contains' | 'Contains';
IS_NULL : 'IS NULL' | 'is null' | 'Is Null';
NOT_NULL : 'NOT NULL' | 'not null' | 'Not Null';
IN : 'IN' | 'in' | 'In';
BETWEEN : 'BETWEEN' | 'between' | 'Between';
AND : 'AND' | 'and' | 'And';
OR : 'OR' | 'or' | 'Or';
NOT : 'NOT' | 'not' | 'Not';
LIMIT : 'LIMIT' | 'limit' | 'Limit';
STARTS_WITH : 'STARTS WITH' | 'starts with' | 'Starts with' | 'Starts With';

// Functions
COUNT : 'COUNT' | 'count' | 'Count';
SUM : 'SUM' | 'sum' | 'Sum';
MIN : 'MIN' | 'min' | 'Min';
MAX : 'MAX' | 'max' | 'Max';
AVG : 'AVG' | 'avg' | 'Avg';
HOUR : 'HOUR' | 'hour' | 'Hour';
MINUTE : 'MINUTE' | 'minute' | 'Minute';
SECOND : 'SECOND' | 'second' | 'Second';
DAY : 'DAY' | 'day' | 'Day';
MONTH : 'MONTH' | 'month' | 'Month';
YEAR : 'YEAR' | 'year' | 'Year';


// Event Properties
TRANSIT_URI : 'TRANSITURI' | 'transituri' | 'TransitUri';
TIMESTAMP : 'TIME' | 'time' | 'Time';
FILESIZE : 'SIZE' | 'size' | 'Size';
TYPE : 'TYPE' | 'type' | 'Type';
COMPONENT_ID : 'COMPONENTID' | 'componentid' | 'ComponentId' | 'componentId' | 'componentID' | 'ComponentID';
UUID : 'UUID' | 'uuid' | 'Uuid';

// Event Types
RECEIVE : 'RECEIVE' | 'receive' | 'Receive';
SEND : 'SEND' | 'send' | 'Send';
DROP : 'DROP' | 'drop' | 'Drop';
CREATE : 'CREATE' | 'create' | 'Create';
EXPIRE : 'EXPIRE' | 'expire' | 'Expire';
FORK : 'FORK' | 'fork' | 'Fork';
JOIN : 'JOIN' | 'join' | 'Join';
CLONE : 'CLONE' | 'clone' | 'Clone';
CONTENT_MODIFIED : 'CONTENT_MODIFIED' | 'content_modified' | 'Content_Modified';
ATTRIBUTES_MODIFIED : 'ATTRIBUTES_MODIFIED' | 'attributes_modified' | 'Attributes_Modified';
ROUTE : 'ROUTE' | 'route' | 'Route';
REPLAY : 'REPLAY' | 'replay' | 'Replay';



IDENTIFIER : (
					('a'..'z' | 'A'..'Z' | '$')
					('a'..'z' | 'A'..'Z' | '$' | '0'..'9' | '_' )*
			);



// STRINGS
STRING_LITERAL
@init{StringBuilder lBuf = new StringBuilder();}
	:
		(
			'"'
				(
					escaped=ESC {lBuf.append(getText());} |
				  	normal = ~( '"' | '\\' | '\n' | '\r' | '\t' ) { lBuf.appendCodePoint(normal);} 
				)*
			'"'
		)
		{
			setText(lBuf.toString());
		}
		|
		(
			'\''
				(
					escaped=ESC {lBuf.append(getText());} |
				  	normal = ~( '\'' | '\\' | '\n' | '\r' | '\t' ) { lBuf.appendCodePoint(normal);} 
				)*
			'\''
		)
		{
			setText(lBuf.toString());
		}
		;


fragment
ESC
	:	'\\'
		(
				'"'		{ setText("\""); }
			|	'\''	{ setText("\'"); }
			|	'r'		{ setText("\r"); }
			|	'n'		{ setText("\n"); }
			|	't'		{ setText("\t"); }
			|	'\\'	{ setText("\\\\"); }
			|	nextChar = ~('"' | '\'' | 'r' | 'n' | 't' | '\\')		
				{
					StringBuilder lBuf = new StringBuilder(); lBuf.append("\\\\").appendCodePoint(nextChar); setText(lBuf.toString());
				}
		)
	;
	

