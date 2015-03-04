/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.provenance.journaling;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.provenance.SearchableFields;
import org.apache.nifi.provenance.StandardProvenanceEventRecord;
import org.apache.nifi.provenance.StoredProvenanceEvent;
import org.apache.nifi.provenance.journaling.config.JournalingRepositoryConfig;
import org.apache.nifi.provenance.lineage.ComputeLineageResult;
import org.apache.nifi.provenance.lineage.ComputeLineageSubmission;
import org.apache.nifi.provenance.lineage.LineageNode;
import org.apache.nifi.provenance.lineage.LineageNodeType;
import org.apache.nifi.provenance.lineage.ProvenanceEventLineageNode;
import org.apache.nifi.provenance.query.ProvenanceResultSet;
import org.apache.nifi.provenance.search.Query;
import org.apache.nifi.provenance.search.QueryResult;
import org.apache.nifi.provenance.search.QuerySubmission;
import org.apache.nifi.provenance.search.SearchTerms;
import org.apache.nifi.provenance.search.SearchableField;
import org.apache.nifi.util.file.FileUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestJournalingProvenanceRepository {

    
    @BeforeClass
    public static void setupLogging() {
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.provenance.journaling", "DEBUG");
    }
    
    @Test
    public void testStoreAndRetrieve() throws IOException {
        final JournalingRepositoryConfig config = new JournalingRepositoryConfig();
        final Map<String, File> containers = new HashMap<>();
        containers.put("container1", new File("target/" + UUID.randomUUID().toString()));
        containers.put("container2", new File("target/" + UUID.randomUUID().toString()));
        config.setContainers(containers);
        config.setPartitionCount(3);
        
        try (final JournalingProvenanceRepository repo = new JournalingProvenanceRepository(config)) {
            repo.initialize(null);
            final Map<String, String> attributes = new HashMap<>();
            
            for (int i=0; i < 10; i++) {
                attributes.put("i", String.valueOf(i));
                repo.registerEvent(TestUtil.generateEvent(i, attributes));
            }
            
            // retrieve records one at a time.
            for (int i=0; i < 10; i++) {
                final StoredProvenanceEvent event = repo.getEvent(i);
                assertNotNull(event);
                assertEquals((long) i, event.getEventId());
                assertEquals("00000000-0000-0000-0000-00000000000" + i, event.getFlowFileUuid());
            }
            
            final List<StoredProvenanceEvent> events = repo.getEvents(0, 1000);
            assertNotNull(events);
            assertEquals(10, events.size());
            for (int i=0; i < 10; i++) {
                final StoredProvenanceEvent event = events.get(i);
                assertNotNull(event);
                assertEquals((long) i, event.getEventId());
                assertEquals("00000000-0000-0000-0000-00000000000" + i, event.getFlowFileUuid());
            }
        } finally {
            for ( final File file : containers.values() ) {
                if ( file.exists() ) {
                    FileUtils.deleteFile(file, true);
                }
            }
        }
    }
    
    
    @Test
    public void testStoreRestartAndRetrieve() throws IOException {
        final JournalingRepositoryConfig config = new JournalingRepositoryConfig();
        final Map<String, File> containers = new HashMap<>();
        containers.put("container1", new File("target/" + UUID.randomUUID().toString()));
        containers.put("container2", new File("target/" + UUID.randomUUID().toString()));
        config.setContainers(containers);
        config.setPartitionCount(3);
        
        try {
            try (final JournalingProvenanceRepository repo = new JournalingProvenanceRepository(config)) {
                repo.initialize(null);
                final Map<String, String> attributes = new HashMap<>();
                
                for (int i=0; i < 10; i++) {
                    attributes.put("i", String.valueOf(i));
                    repo.registerEvent(TestUtil.generateEvent(i, attributes));
                }
                
                assertEquals(10L, repo.getMaxEventId().longValue());
            }
    
            try (final JournalingProvenanceRepository repo = new JournalingProvenanceRepository(config)) {
                repo.initialize(null);

                assertEquals(10L, repo.getMaxEventId().longValue());

                // retrieve records one at a time.
                for (int i=0; i < 10; i++) {
                    final StoredProvenanceEvent event = repo.getEvent(i);
                    assertNotNull(event);
                    assertEquals((long) i, event.getEventId());
                    assertEquals("00000000-0000-0000-0000-00000000000" + i, event.getFlowFileUuid());
                }
                
                final List<StoredProvenanceEvent> events = repo.getEvents(0, 1000);
                assertNotNull(events);
                assertEquals(10, events.size());
                for (int i=0; i < 10; i++) {
                    final StoredProvenanceEvent event = events.get(i);
                    assertNotNull(event);
                    assertEquals((long) i, event.getEventId());
                    assertEquals("00000000-0000-0000-0000-00000000000" + i, event.getFlowFileUuid());
                }
            }
        } finally {
            for ( final File file : containers.values() ) {
                if ( file.exists() ) {
                    FileUtils.deleteFile(file, true);
                }
            }
        }
    }
    
    
    @Test
    public void testStoreRestartRetrieveAndExpireOnTime() throws IOException, InterruptedException {
        final JournalingRepositoryConfig config = new JournalingRepositoryConfig();
        final Map<String, File> containers = new HashMap<>();
        containers.put("container1", new File("target/" + UUID.randomUUID().toString()));
        containers.put("container2", new File("target/" + UUID.randomUUID().toString()));
        config.setContainers(containers);
        config.setPartitionCount(3);
        
        try {
            try (final JournalingProvenanceRepository repo = new JournalingProvenanceRepository(config)) {
                repo.initialize(null);
                final Map<String, String> attributes = new HashMap<>();
                
                for (int i=0; i < 10; i++) {
                    attributes.put("i", String.valueOf(i));
                    repo.registerEvent(TestUtil.generateEvent(i, attributes));
                }
                
                assertEquals(10L, repo.getMaxEventId().longValue());
            }
    
            config.setExpirationFrequency(1, TimeUnit.SECONDS);
            try (final JournalingProvenanceRepository repo = new JournalingProvenanceRepository(config)) {
                repo.initialize(null);

                assertEquals(10L, repo.getMaxEventId().longValue());

                // retrieve records one at a time.
                for (int i=0; i < 10; i++) {
                    final StoredProvenanceEvent event = repo.getEvent(i);
                    assertNotNull(event);
                    assertEquals((long) i, event.getEventId());
                    assertEquals("00000000-0000-0000-0000-00000000000" + i, event.getFlowFileUuid());
                }
                
                final List<StoredProvenanceEvent> events = repo.getEvents(0, 1000);
                assertNotNull(events);
                assertEquals(10, events.size());
                for (int i=0; i < 10; i++) {
                    final StoredProvenanceEvent event = events.get(i);
                    assertNotNull(event);
                    assertEquals((long) i, event.getEventId());
                    assertEquals("00000000-0000-0000-0000-00000000000" + i, event.getFlowFileUuid());
                }
                
                // wait a bit for the events to be expired
                TimeUnit.SECONDS.sleep(2L);
                
                // retrieve records one at a time.
                for (int i=0; i < 10; i++) {
                    final StoredProvenanceEvent event = repo.getEvent(i);
                    assertNull("Event " + i + " still exists", event);
                }
                
                final List<StoredProvenanceEvent> allEvents = repo.getEvents(0, 1000);
                assertNotNull(allEvents);
                assertEquals(0, allEvents.size());
            }
        } finally {
            for ( final File file : containers.values() ) {
                if ( file.exists() ) {
                    FileUtils.deleteFile(file, true);
                }
            }
        }
    }
    
    
    @Test
    public void testExpireOnSize() throws IOException, InterruptedException {
        final JournalingRepositoryConfig config = new JournalingRepositoryConfig();
        final Map<String, File> containers = new HashMap<>();
        containers.put("container1", new File("target/" + UUID.randomUUID().toString()));
        containers.put("container2", new File("target/" + UUID.randomUUID().toString()));
        config.setContainers(containers);
        config.setPartitionCount(3);
        config.setMaxStorageCapacity(1024L * 50);
        config.setEventExpiration(2, TimeUnit.SECONDS);
        config.setExpirationFrequency(1, TimeUnit.SECONDS);
        config.setJournalRolloverPeriod(1, TimeUnit.SECONDS);
        config.setCompressOnRollover(false);
        
        try {
            try (final JournalingProvenanceRepository repo = new JournalingProvenanceRepository(config)) {
                repo.initialize(null);
                final Map<String, String> attributes = new HashMap<>();
                
                final int numEventsToInsert = 1000;
                for (int i=0; i < numEventsToInsert; i++) {
                    attributes.put("i", String.valueOf(i));
                    repo.registerEvent(TestUtil.generateEvent(i, attributes));
                }

                final List<StoredProvenanceEvent> eventsBeforeExpire = repo.getEvents(0, numEventsToInsert * 2);
                assertNotNull(eventsBeforeExpire);
                assertEquals(numEventsToInsert, eventsBeforeExpire.size());
                
                // wait a bit for expiration to occur
                TimeUnit.SECONDS.sleep(3L);
                
                // generate an event for each partition to force a rollover of the journals
                for (int i=0; i < config.getPartitionCount(); i++) {
                    repo.registerEvent(TestUtil.generateEvent(100000L));
                }

                TimeUnit.SECONDS.sleep(1L);
  
                // retrieve records one at a time.
                for (int i=0; i < numEventsToInsert; i++) {
                    final StoredProvenanceEvent event = repo.getEvent(i);
                    assertNull("Event " + i + " still exists", event);
                }
                
                final List<StoredProvenanceEvent> eventsAfterExpire = repo.getEvents(0, numEventsToInsert * 2);
                assertNotNull(eventsAfterExpire);
                assertEquals(3, eventsAfterExpire.size());
            }
        } finally {
            for ( final File file : containers.values() ) {
                if ( file.exists() ) {
                    FileUtils.deleteFile(file, true);
                }
            }
        }
    }
    
    
    
    @Test(timeout=10000)
    public void testSearchByUUID() throws IOException, InterruptedException {
        final JournalingRepositoryConfig config = new JournalingRepositoryConfig();
        final Map<String, File> containers = new HashMap<>();
        containers.put("container1", new File("target/" + UUID.randomUUID().toString()));
        containers.put("container2", new File("target/" + UUID.randomUUID().toString()));
        config.setContainers(containers);
        
        config.setPartitionCount(3);
        config.setSearchableFields(Arrays.asList(new SearchableField[] {
                SearchableFields.FlowFileUUID
        }));
        
        try (final JournalingProvenanceRepository repo = new JournalingProvenanceRepository(config)) {
            repo.initialize(null);
            
            final Map<String, String> attributes = new HashMap<>();
            
            for (int i=0; i < 10; i++) {
                attributes.put("i", String.valueOf(i));
                repo.registerEvent(TestUtil.generateEvent(i, attributes));
            }
            
            final Query query = new Query("query");
            query.addSearchTerm(SearchTerms.newSearchTerm(SearchableFields.FlowFileUUID, "00000000-0000-0000-0000-000000000005"));
            final QuerySubmission submission = repo.submitQuery(query);
            assertNotNull(submission);
            
            final QueryResult result = submission.getResult();
            while ( !result.isFinished() ) {
                Thread.sleep(50L);
            }
            
            assertNull(result.getError());
            final List<StoredProvenanceEvent> matches = result.getMatchingEvents();
            assertNotNull(matches);
            assertEquals(1, matches.size());
            
            final StoredProvenanceEvent event = matches.get(0);
            assertEquals(5, event.getEventId());
            assertEquals("00000000-0000-0000-0000-000000000005", event.getFlowFileUuid());
            System.out.println("Query time was " + result.getQueryTime() + " millis");
        } finally {
            for ( final File file : containers.values() ) {
                FileUtils.deleteFile(file, true);
            }
        }
    }
    
    @Test()
    public void testSearchByUUIDTextQuery() throws IOException, InterruptedException {
        final JournalingRepositoryConfig config = new JournalingRepositoryConfig();
        final Map<String, File> containers = new HashMap<>();
        containers.put("container1", new File("target/" + UUID.randomUUID().toString()));
        config.setContainers(containers);
        
        config.setPartitionCount(1);
        config.setSearchableFields(Arrays.asList(new SearchableField[] {
                SearchableFields.FlowFileUUID
        }));
        
        try (final JournalingProvenanceRepository repo = new JournalingProvenanceRepository(config)) {
            repo.initialize(null);
            
            final Map<String, String> attributes = new HashMap<>();
            
            final long start = System.nanoTime();
            for (int i=0; i < 10; i++) {
                attributes.put("i", String.valueOf(i));
                repo.registerEvent(TestUtil.generateEvent(i, attributes));
            }
            final long registerFinish = System.nanoTime();
            
            // Ensure that we get the Events, even though we are querying for Event.Time because selectMatchingEvents
            // only takes into account the WHERE clause
            final Iterator<? extends StoredProvenanceEvent> itr = repo.selectMatchingEvents("SELECT Event.Time WHERE Event.uuid = '00000000-0000-0000-0000-000000000005'", new AtomicLong(0L));
            assertTrue(itr.hasNext());
            final StoredProvenanceEvent event = itr.next();
            assertNotNull(event);
            assertFalse(itr.hasNext());
            
            final long searchFinish = System.nanoTime();
            assertEquals(5, event.getEventId());
            assertEquals("00000000-0000-0000-0000-000000000005", event.getFlowFileUuid());
            
            System.out.println("Register records: " + TimeUnit.NANOSECONDS.toMillis(registerFinish - start) + " millis");
            System.out.println("Query records: " + TimeUnit.NANOSECONDS.toMillis(searchFinish - registerFinish) + " millis");
        } catch (final Exception e) {
            e.printStackTrace();
        } finally {
            for ( final File file : containers.values() ) {
                FileUtils.deleteFile(file, true);
            }
        }
    }
    
    
    @Test()
    public void testSearchFieldTextQuery() throws IOException, InterruptedException {
        final JournalingRepositoryConfig config = new JournalingRepositoryConfig();
        final Map<String, File> containers = new HashMap<>();
        containers.put("container1", new File("target/" + UUID.randomUUID().toString()));
        containers.put("container2", new File("target/" + UUID.randomUUID().toString()));
        config.setContainers(containers);
        
        config.setPartitionCount(3);
        config.setSearchableFields(Arrays.asList(new SearchableField[] {
                SearchableFields.FlowFileUUID
        }));
        
        try (final JournalingProvenanceRepository repo = new JournalingProvenanceRepository(config)) {
            repo.initialize(null);
            
            final Map<String, String> attributes = new HashMap<>();
            
            final long start = System.nanoTime();
            for (int i=0; i < 10; i++) {
                attributes.put("i", String.valueOf(i));
                repo.registerEvent(TestUtil.generateEvent(i, attributes));
            }
            final long registerFinish = System.nanoTime();
            
            final ProvenanceResultSet rs = repo.query("SELECT Event.uuid WHERE Event.uuid = '00000000-0000-0000-0000-000000000005'");
            assertTrue(rs.hasNext());
            final List<?> cols = rs.next();
            assertEquals(1, cols.size());
            assertEquals(String.class, rs.getReturnType().get(0));
            final Object firstCol = cols.get(0);
            assertEquals("00000000-0000-0000-0000-000000000005", firstCol);
            assertFalse(rs.hasNext());
            
            final long searchFinish = System.nanoTime();
            System.out.println("Register records: " + TimeUnit.NANOSECONDS.toMillis(registerFinish - start) + " millis");
            System.out.println("Query records: " + TimeUnit.NANOSECONDS.toMillis(searchFinish - registerFinish) + " millis");
        } finally {
            for ( final File file : containers.values() ) {
                FileUtils.deleteFile(file, true);
            }
        }
    }
    
    @Test()
    public void testSearchEventTextQuery() throws IOException, InterruptedException {
        final JournalingRepositoryConfig config = new JournalingRepositoryConfig();
        final Map<String, File> containers = new HashMap<>();
        containers.put("container1", new File("target/" + UUID.randomUUID().toString()));
        containers.put("container2", new File("target/" + UUID.randomUUID().toString()));
        config.setContainers(containers);
        
        config.setPartitionCount(3);
        config.setSearchableFields(Arrays.asList(new SearchableField[] {
                SearchableFields.FlowFileUUID
        }));
        
        try (final JournalingProvenanceRepository repo = new JournalingProvenanceRepository(config)) {
            repo.initialize(null);
            
            final Map<String, String> attributes = new HashMap<>();
            
            final long start = System.nanoTime();
            for (int i=0; i < 10; i++) {
                attributes.put("i", String.valueOf(i));
                repo.registerEvent(TestUtil.generateEvent(i, attributes));
            }
            final long registerFinish = System.nanoTime();
            
            final ProvenanceResultSet rs = repo.query("SELECT Event WHERE Event.uuid = '00000000-0000-0000-0000-000000000005'");
            assertTrue(rs.hasNext());
            final List<?> cols = rs.next();
            assertEquals(1, cols.size());
            assertTrue(ProvenanceEventRecord.class.isAssignableFrom(rs.getReturnType().get(0)));
            final ProvenanceEventRecord firstCol = (ProvenanceEventRecord) cols.get(0);
            assertEquals("00000000-0000-0000-0000-000000000005", firstCol.getFlowFileUuid());
            assertEquals("5", firstCol.getAttribute("i"));
            assertEquals("5", firstCol.getAttributes().get("i"));
            assertEquals("5", firstCol.getUpdatedAttributes().get("i"));
            assertFalse(rs.hasNext());
            
            final long searchFinish = System.nanoTime();
            System.out.println("Register records: " + TimeUnit.NANOSECONDS.toMillis(registerFinish - start) + " millis");
            System.out.println("Query records: " + TimeUnit.NANOSECONDS.toMillis(searchFinish - registerFinish) + " millis");
        } finally {
            for ( final File file : containers.values() ) {
                FileUtils.deleteFile(file, true);
            }
        }
    }
    
    
    @Test()
    public void testSearchEventTextQueryAgainstMany() throws IOException, InterruptedException {
        final JournalingRepositoryConfig config = new JournalingRepositoryConfig();
        final Map<String, File> containers = new HashMap<>();
        containers.put("container1", new File("target/" + UUID.randomUUID().toString()));
        containers.put("container2", new File("target/" + UUID.randomUUID().toString()));
        config.setContainers(containers);
        
        config.setPartitionCount(3);
        config.setSearchableFields(Arrays.asList(new SearchableField[] {
                SearchableFields.FlowFileUUID
        }));
        
        try (final JournalingProvenanceRepository repo = new JournalingProvenanceRepository(config)) {
            repo.initialize(null);
            
            final Map<String, String> attributes = new HashMap<>();
            
            final long start = System.nanoTime();
            final List<ProvenanceEventRecord> events = new ArrayList<>(1000);
            for (int i=0; i < 100000; i++) {
                attributes.put("i", String.valueOf(i));
                final ProvenanceEventRecord event = TestUtil.generateEvent(i, attributes);
                events.add(event);
                if ( events.size() % 1000 == 0 ) {
                    repo.registerEvents(events);
                    events.clear();
                }
            }
            final long registerFinish = System.nanoTime();
            
            final ProvenanceResultSet rs = repo.query("SELECT Event WHERE Event.uuid = '00000000-0000-0000-0000-000000000005'");
            assertTrue(rs.hasNext());
            final List<?> cols = rs.next();
            assertEquals(1, cols.size());
            assertTrue(ProvenanceEventRecord.class.isAssignableFrom(rs.getReturnType().get(0)));
            final ProvenanceEventRecord firstCol = (ProvenanceEventRecord) cols.get(0);
            assertEquals("00000000-0000-0000-0000-000000000005", firstCol.getFlowFileUuid());
            assertFalse(rs.hasNext());
            
            final long searchFinish = System.nanoTime();
            System.out.println("Register records: " + TimeUnit.NANOSECONDS.toMillis(registerFinish - start) + " millis");
            System.out.println("Query records: " + TimeUnit.NANOSECONDS.toMillis(searchFinish - registerFinish) + " millis");
        } finally {
            for ( final File file : containers.values() ) {
                FileUtils.deleteFile(file, true);
            }
        }
    }
    
    
    @Test()
    public void testSearchManyEventsTextQueryAgainstMany() throws IOException, InterruptedException {
        final JournalingRepositoryConfig config = new JournalingRepositoryConfig();
        final Map<String, File> containers = new HashMap<>();
        containers.put("container1", new File("target/" + UUID.randomUUID().toString()));
        containers.put("container2", new File("target/" + UUID.randomUUID().toString()));
        config.setContainers(containers);
        
        config.setPartitionCount(3);
        config.setSearchableFields(Arrays.asList(new SearchableField[] {
                SearchableFields.FlowFileUUID
        }));
        
        try (final JournalingProvenanceRepository repo = new JournalingProvenanceRepository(config)) {
            repo.initialize(null);
            
            final Map<String, String> attributes = new HashMap<>();
            
            final long start = System.nanoTime();
            final List<ProvenanceEventRecord> events = new ArrayList<>(1000);
            for (int i=0; i < 100000; i++) {
                attributes.put("i", String.valueOf(i));
                final ProvenanceEventRecord event = TestUtil.generateEvent(i, attributes);
                events.add(event);
                if ( events.size() % 1000 == 0 ) {
                    repo.registerEvents(events);
                    events.clear();
                }
            }
            final long registerFinish = System.nanoTime();
            
            final ProvenanceResultSet rs = repo.query("SELECT Event");
            for (int i=0; i < 100000; i++) {
                assertTrue(rs.hasNext());
                final List<?> cols = rs.next();
                assertEquals(1, cols.size());
            }
            assertFalse(rs.hasNext());
            
            final long searchFinish = System.nanoTime();
            System.out.println("Register records: " + TimeUnit.NANOSECONDS.toMillis(registerFinish - start) + " millis");
            System.out.println("Query records: " + TimeUnit.NANOSECONDS.toMillis(searchFinish - registerFinish) + " millis");
        } catch (final Exception e) {
            e.printStackTrace();
        } finally {
            for ( final File file : containers.values() ) {
                FileUtils.deleteFile(file, true);
            }
        }
    }
    
    
    @Test()
    public void testAggregateQueryAgainstMany() throws IOException, InterruptedException {
        final JournalingRepositoryConfig config = new JournalingRepositoryConfig();
        final Map<String, File> containers = new HashMap<>();
        containers.put("container1", new File("target/" + UUID.randomUUID().toString()));
        containers.put("container2", new File("target/" + UUID.randomUUID().toString()));
        config.setContainers(containers);
        
        config.setPartitionCount(3);
        config.setSearchableFields(Arrays.asList(new SearchableField[] {
                SearchableFields.FlowFileUUID
        }));
        
        try (final JournalingProvenanceRepository repo = new JournalingProvenanceRepository(config)) {
            repo.initialize(null);
            
            final Map<String, String> attributes = new HashMap<>();
            
            final long start = System.nanoTime();
            final List<ProvenanceEventRecord> events = new ArrayList<>(1000);
            for (int i=0; i < 100000; i++) {
                attributes.put("i", String.valueOf(i));
                final ProvenanceEventRecord event = TestUtil.generateEvent(i, attributes);
                events.add(event);
                if ( events.size() % 1000 == 0 ) {
                    repo.registerEvents(events);
                    events.clear();
                }
            }
            final long registerFinish = System.nanoTime();
            
            final ProvenanceResultSet rs = repo.query("SELECT SUM(Event.Size), AVG(Event.Size)");
            assertTrue(rs.hasNext());
            assertEquals(Long.class, rs.getReturnType().get(0));
            assertEquals(Double.class, rs.getReturnType().get(1));
            final List<?> cols = rs.next();
            assertEquals(100000L, cols.get(0));
            assertEquals(1D, cols.get(1));
            assertFalse(rs.hasNext());
            
            final long searchFinish = System.nanoTime();
            System.out.println("Register records: " + TimeUnit.NANOSECONDS.toMillis(registerFinish - start) + " millis");
            System.out.println("Query records: " + TimeUnit.NANOSECONDS.toMillis(searchFinish - registerFinish) + " millis");
        } catch (final Exception e) {
            e.printStackTrace();
            Assert.fail(e.toString());
        } finally {
            for ( final File file : containers.values() ) {
                FileUtils.deleteFile(file, true);
            }
        }
    }
    
    
    @Test(timeout=10000)
    public void testReceiveDropLineage() throws IOException, InterruptedException {
        final JournalingRepositoryConfig config = new JournalingRepositoryConfig();
        final Map<String, File> containers = new HashMap<>();
        containers.put("container1", new File("target/" + UUID.randomUUID().toString()));
        containers.put("container2", new File("target/" + UUID.randomUUID().toString()));
        config.setContainers(containers);
        
        config.setPartitionCount(3);
        config.setSearchableFields(Arrays.asList(new SearchableField[] {
                SearchableFields.FlowFileUUID
        }));
        
        try (final JournalingProvenanceRepository repo = new JournalingProvenanceRepository(config)) {
            repo.initialize(null);
        
            final String uuid = "00000000-0000-0000-0000-000000000001";
            final Map<String, String> attributes = new HashMap<>();
            attributes.put("abc", "xyz");
            attributes.put("uuid", uuid);
            attributes.put("filename", "file-" + uuid);
    
            final StandardProvenanceEventRecord.Builder builder = new StandardProvenanceEventRecord.Builder()
                .setEventType(ProvenanceEventType.RECEIVE)
                .setFlowFileUUID(uuid)
                .setComponentType("Unit Test")
                .setComponentId(UUID.randomUUID().toString())
                .setEventTime(System.currentTimeMillis())
                .setFlowFileEntryDate(System.currentTimeMillis() - 1000L)
                .setLineageStartDate(System.currentTimeMillis() - 2000L)
                .setCurrentContentClaim(null, null, null, null, 0L)
                .setAttributes(null, attributes == null ? Collections.<String, String>emptyMap() : attributes);

            builder.setTransitUri("nifi://unit-test");
            attributes.put("uuid", uuid);
            builder.setComponentId("1234");
            builder.setComponentType("dummy processor");
    
            // Add RECEIVE Event
            repo.registerEvent(builder.build());
    
            builder.setEventTime(System.currentTimeMillis() + 1);
            builder.setEventType(ProvenanceEventType.DROP);
            builder.setTransitUri(null);
            
            // Add DROP event
            repo.registerEvent(builder.build());
            
            // register unrelated even to make sure we don't get this one.
            builder.setFlowFileUUID("00000000-0000-0000-0000-000000000002");
            repo.registerEvent(builder.build());
            
            final ComputeLineageSubmission submission = repo.submitLineageComputation(uuid);
            assertNotNull(submission);
            
            final ComputeLineageResult result = submission.getResult();
            while ( !result.isFinished() ) {
                Thread.sleep(50L);
            }
            
            assertNull(result.getError());
            
            final List<LineageNode> nodes = result.getNodes();
            assertEquals(3, nodes.size());  // RECEIVE, FlowFile node, DROP
            
            int receiveCount = 0;
            int dropCount = 0;
            int flowFileNodeCount = 0;
            for ( final LineageNode node : nodes ) {
                assertEquals(uuid, node.getFlowFileUuid());
                
                if ( LineageNodeType.PROVENANCE_EVENT_NODE.equals(node.getNodeType()) ) {
                    final ProvenanceEventLineageNode eventNode = (ProvenanceEventLineageNode) node;
                    if ( eventNode.getEventType() == ProvenanceEventType.RECEIVE ) {
                        receiveCount++;
                    } else if ( eventNode.getEventType() == ProvenanceEventType.DROP ) {
                        dropCount++;
                    }
                } else {
                    flowFileNodeCount++;
                }
            }
            
            assertEquals(1, receiveCount);
            assertEquals(1, dropCount);
            assertEquals(1, flowFileNodeCount);
        } finally {
            for ( final File file : containers.values() ) {
                FileUtils.deleteFile(file, true);
            }
        }
    }
    
}
