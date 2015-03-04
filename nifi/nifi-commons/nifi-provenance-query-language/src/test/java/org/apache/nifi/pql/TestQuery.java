package org.apache.nifi.pql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.provenance.StandardProvenanceEventRecord;
import org.apache.nifi.provenance.VolatileProvenanceRepository;
import org.apache.nifi.provenance.query.ProvenanceResultSet;
import org.apache.nifi.util.NiFiProperties;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestQuery {

	private ProvenanceEventRepository repo;
	
	@Before
	public void setup() {
		System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, "src/test/resources/nifi.properties");
		repo = new VolatileProvenanceRepository();
	}
	
	
	private void createRecords() throws IOException {
		final Map<String, String> previousAttributes = new HashMap<>();
		previousAttributes.put("filename", "xyz");
		
		final Map<String, String> updatedAttributes = new HashMap<>();
		updatedAttributes.put("filename", "xyz.txt");
		updatedAttributes.put("mime.type", "text/plain");
		
		final StandardProvenanceEventRecord.Builder recordBuilder = new StandardProvenanceEventRecord.Builder();
		recordBuilder.setAttributes(previousAttributes, Collections.<String, String>emptyMap())
			.setComponentId("000")
			.setComponentType("MyComponent")
			.setEventType(ProvenanceEventType.RECEIVE)
			.setFlowFileEntryDate(System.currentTimeMillis())
			.setFlowFileUUID("1234")
			.setTransitUri("https://localhost:80/nifi");
		
		
		recordBuilder.setCurrentContentClaim("container", "section", "1", 0L, 100L);
		repo.registerEvent(recordBuilder.build());
		
		
		recordBuilder.setAttributes(previousAttributes, updatedAttributes);
		recordBuilder.setCurrentContentClaim("container", "section", "2", 0L, 1024 * 1024L);
		repo.registerEvent(recordBuilder.build());
	}
	
	
	private void createRecords(final int records, final ProvenanceEventType type, final long sleep) throws IOException {
		final Map<String, String> previousAttributes = new HashMap<>();
		previousAttributes.put("filename", "xyz");
		
		final Map<String, String> updatedAttributes = new HashMap<>();
		updatedAttributes.put("filename", "xyz.txt");
		updatedAttributes.put("mime.type", "text/plain");
		
		final StandardProvenanceEventRecord.Builder recordBuilder = new StandardProvenanceEventRecord.Builder();
		recordBuilder.setAttributes(previousAttributes, Collections.<String, String>emptyMap())
			.setComponentType("MyComponent")
			.setEventType(type)
			.setFlowFileEntryDate(System.currentTimeMillis())
			.setFlowFileUUID("1234")
			.setTransitUri("https://localhost:80/nifi");
		
		final long now = System.currentTimeMillis();
		for (int i=0; i < records; i++) {
			recordBuilder.setCurrentContentClaim("container", "section", String.valueOf(i), 0L, 100L);
			final Map<String, String> attr = new HashMap<>(updatedAttributes);
			attr.put("i", String.valueOf(i));
			recordBuilder.setAttributes(previousAttributes, attr);
			recordBuilder.setFlowFileEntryDate(System.currentTimeMillis());
			recordBuilder.setEventTime(now + (i * sleep));
			recordBuilder.setComponentId(UUID.randomUUID().toString());
			
			repo.registerEvent(recordBuilder.build());
		}
	}
	
	@Test
	public void testCompilationManually() {
		System.out.println(ProvenanceQuery.compile("SELECT R.TransitUri FROM *"));
		System.out.println(ProvenanceQuery.compile("SELECT R['filename'] FROM RECEIVE, SEND;"));
		System.out.println(ProvenanceQuery.compile("SELECT Event FROM RECEIVE ORDER BY Event['filename'];"));
		
//		System.out.println(Query.compile("SELECT Event FROM RECEIVE WHERE ((Event.TransitUri <> 'http') OR (Event['filename'] = '1.txt')) and (Event.Size > 1000 or Event.Size between 1 AND 4);"));
		
		System.out.println(ProvenanceQuery.compile("SELECT SUM(Event.size) FROM RECEIVE"));
	}
	
	
	@Test
	public void testSumAverage() throws IOException {
		createRecords();
		dump(ProvenanceQuery.execute("SELECT Event", repo));
		
		final ProvenanceQuery query = ProvenanceQuery.compile("SELECT SUM(Event.Size), AVG(Event.Size) FROM RECEIVE WHERE Event.TransitUri = 'https://localhost:80/nifi'");
		
		final ProvenanceResultSet rs = query.execute(repo);
		dump(rs);
		
		dump(ProvenanceQuery.execute("SELECT Event.TransitUri", repo));
		dump(ProvenanceQuery.execute("SELECT Event['mime.type'], Event['filename']", repo));
		dump(ProvenanceQuery.execute("SELECT Event['filename'], SUM(Event.size) GROUP BY Event['filename']", repo));
	}
	
	
	@Test
	public void testGroupBy() throws IOException {
		createRecords(200000, ProvenanceEventType.RECEIVE, 0L);
		createRecords(2, ProvenanceEventType.SEND, 0L);
		
		ProvenanceResultSet rs = ProvenanceQuery.execute("SELECT Event['filename'], COUNT(Event), Event.Type GROUP BY Event['filename'], Event.Type", repo);
		dump(rs);
		
		rs = ProvenanceQuery.execute("SELECT Event['filename'], COUNT(Event), Event.Type GROUP BY Event['filename'], Event.Type", repo);
		
		int receiveRows = 0;
		int sendRows = 0;
		while (rs.hasNext()) {
		    final List<?> cols = rs.next();
		    final ProvenanceEventType type = (ProvenanceEventType) cols.get(2);
		    if ( type == ProvenanceEventType.RECEIVE ) {
		        receiveRows++;
		        assertEquals("xyz.txt", cols.get(0));
		        assertEquals(200000L, cols.get(1));
		    } else if ( type == ProvenanceEventType.SEND ) {
		        sendRows++;
		        assertEquals("xyz.txt", cols.get(0));
		        assertEquals(2L, cols.get(1));
		    } else {
		        Assert.fail("Event type was " + type);
		    }
		}
		
		assertEquals(1, receiveRows);
		assertEquals(1, sendRows);
	}
	
	
	@Test
    public void testAverageGroupBy() throws IOException {
        createRecords(200000, ProvenanceEventType.RECEIVE, 1L);
        createRecords(5000, ProvenanceEventType.SEND, 1L);
        
        dump(ProvenanceQuery.execute("SELECT AVG(Event.Size), Event.Type GROUP BY SECOND(Event.Time), Event.Type", repo));
    }
	
	
	@Test
	public void testSelectSeveralRecords() throws IOException {
		createRecords(2000, ProvenanceEventType.SEND, 1L);
		createRecords(200, ProvenanceEventType.RECEIVE, 1L);
		dump(ProvenanceQuery.execute(
				  "SELECT SECOND(Event.Time), Event.Type, SUM(Event.Size), COUNT(Event) "
				+ "FROM SEND, RECEIVE "
				+ "GROUP BY SECOND(Event.Time), Event.Type"
				, repo));
	}

	@Test
	public void testNot() throws IOException {
		createRecords(2000, ProvenanceEventType.SEND, 0L);
		createRecords(200, ProvenanceEventType.RECEIVE, 0L);

		dump(ProvenanceQuery.execute("SELECT Event.Type, COUNT(Event) WHERE NOT(Event.Type = 'SEND')", repo));
		dump(ProvenanceQuery.execute("SELECT Event.Type, COUNT(Event) WHERE NOT(Event.Type = 'RECEIVE')", repo));
		dump(ProvenanceQuery.execute("SELECT Event.Type, COUNT(Event) WHERE NOT(NOT( Event.Type = 'SEND'))", repo));
	}
	
	
	@Test
	public void testOrderByField() throws IOException {
		createRecords(2000, ProvenanceEventType.SEND, 1L);
		
		dump(ProvenanceQuery.execute("SELECT Event.Time, Event.ComponentId ORDER BY Event.ComponentId LIMIT 15", repo));
		dump(ProvenanceQuery.execute("SELECT Event.Time, Event.ComponentId ORDER BY Event.Time DESC LIMIT 15", repo));
	}
	

	@Test
	public void testOrderByGroupedField() throws IOException {
		createRecords(2, ProvenanceEventType.SEND, 0L);
		createRecords(5, ProvenanceEventType.RECEIVE, 0L);
		
		dump(ProvenanceQuery.execute("SELECT Event.Type, SUM(Event.Size) GROUP BY Event.Type ORDER BY SUM(Event.Size) DESC", repo));
		
		ProvenanceResultSet rs = ProvenanceQuery.execute("SELECT Event.Type, SUM(Event.Size) GROUP BY Event.Type ORDER BY SUM(Event.Size) DESC", repo);
		
		assertTrue( rs.hasNext() );
		List<?> values = rs.next();
		assertEquals("RECEIVE", values.get(0).toString());
		assertEquals(500L, values.get(1));
		
		assertTrue( rs.hasNext() );
		values = rs.next();
		assertEquals("SEND", values.get(0).toString());
		assertEquals(200L, values.get(1));
		
		assertFalse( rs.hasNext() );
		
		
		rs = ProvenanceQuery.execute("SELECT Event.Type, SUM(Event.Size) GROUP BY Event.Type ORDER BY SUM(Event.Size) ASC", repo);
		
		assertTrue( rs.hasNext() );
		values = rs.next();
		assertEquals("SEND", values.get(0).toString());
		assertEquals(200L, values.get(1));
		
		assertTrue( rs.hasNext() );
		values = rs.next();
		assertEquals("RECEIVE", values.get(0).toString());
		assertEquals(500L, values.get(1));
		
		assertFalse( rs.hasNext() );
	}
	
	
	@Test
	public void testOrderByFieldAndGroupedValue() throws IOException {
		createRecords(3, ProvenanceEventType.SEND, 0L);
		createRecords(5, ProvenanceEventType.RECEIVE, 0L);
		createRecords(3, ProvenanceEventType.ATTRIBUTES_MODIFIED, 0L);
		
		final String query = "SELECT Event.Type, SUM(Event.Size) GROUP BY Event.Type ORDER BY SUM(Event.Size) DESC, Event.Type";
		dump(ProvenanceQuery.execute(query, repo));
		
		ProvenanceResultSet rs = ProvenanceQuery.execute(query, repo);
		
		assertTrue( rs.hasNext() );
		List<?> vals = rs.next();
		assertEquals(2, vals.size());
		assertEquals("RECEIVE", vals.get(0).toString());
		assertEquals(500L, vals.get(1));
		
		assertTrue( rs.hasNext() );
		vals = rs.next();
		assertEquals(2, vals.size());
		assertEquals("ATTRIBUTES_MODIFIED", vals.get(0).toString());
		assertEquals(300L, vals.get(1));
		
		assertTrue( rs.hasNext() );
		vals = rs.next();
		assertEquals(2, vals.size());
		assertEquals("SEND", vals.get(0).toString());
		assertEquals(300L, vals.get(1));
		
		assertFalse( rs.hasNext() );
	}
	
	
	@Test
	public void testAndsOrs() throws IOException {

		final Map<String, String> previousAttributes = new HashMap<>();
		previousAttributes.put("filename", "xyz");
		
		final Map<String, String> updatedAttributes = new HashMap<>();
		updatedAttributes.put("filename", "xyz.txt");
		updatedAttributes.put("mime.type", "text/plain");
		updatedAttributes.put("abc", "cba");
		updatedAttributes.put("123", "321");
		
		final StandardProvenanceEventRecord.Builder recordBuilder = new StandardProvenanceEventRecord.Builder();
		recordBuilder.setAttributes(previousAttributes, Collections.<String, String>emptyMap())
			.setComponentId("000")
			.setComponentType("MyComponent")
			.setEventType(ProvenanceEventType.SEND)
			.setFlowFileEntryDate(System.currentTimeMillis())
			.setFlowFileUUID("1234")
			.setCurrentContentClaim("container", "section", "1", 0L, 100L)
			.setAttributes(previousAttributes, updatedAttributes)
			.setTransitUri("https://localhost:80/nifi");

		repo.registerEvent(recordBuilder.build());
		
		final String queryString = "SELECT Event "
				+ "WHERE "
				+ "( "
				+ "	 Event['filename'] = 'xyz.txt' "
				+ "		OR "
				+ "  Event['mime.type'] = 'ss' "
				+ ") "
				+ "AND "
				+ "( "
				+ "  Event['abc'] = 'cba' "
				+ "		OR "
				+ "	 Event['123'] = '123' "
				+ ")";
		System.out.println(queryString);
		
		final ProvenanceQuery query = ProvenanceQuery.compile(queryString);
		
		System.out.println(query.getWhereClause());
		
		ProvenanceResultSet rs = query.execute(repo);
		assertTrue(rs.hasNext());
		rs.next();
		assertFalse(rs.hasNext());
		
		
		
		updatedAttributes.put("filename", "xxyz");
		repo = new VolatileProvenanceRepository();
		recordBuilder.setAttributes(previousAttributes, updatedAttributes);
		repo.registerEvent(recordBuilder.build());
		
		rs = query.execute(repo);
		assertFalse(rs.hasNext());


		
		updatedAttributes.put("filename", "xyz.txt");
		updatedAttributes.put("123", "123");
		repo = new VolatileProvenanceRepository();
		recordBuilder.setAttributes(previousAttributes, updatedAttributes);
		repo.registerEvent(recordBuilder.build());

		rs = query.execute(repo);
		assertTrue(rs.hasNext());
		rs.next();
		assertFalse(rs.hasNext());
		
	}
	
	private void dump(final ProvenanceResultSet rs) {
		System.out.println(rs.getLabels());
		while (rs.hasNext()) {
			System.out.println(rs.next());
		}
		
		System.out.println("\n\n\n");
	}
	
}
