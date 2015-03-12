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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.provenance.SearchableFields;
import org.apache.nifi.provenance.StorageLocation;
import org.apache.nifi.provenance.StoredProvenanceEvent;
import org.apache.nifi.provenance.journaling.exception.EventNotFoundException;
import org.apache.nifi.provenance.journaling.index.IndexedFieldNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LazyInitializedProvenanceEvent implements StoredProvenanceEvent {
    private static final Logger logger = LoggerFactory.getLogger(LazyInitializedProvenanceEvent.class);
    
    private final ProvenanceEventRepository repo;
    private final StorageLocation storageLocation;
    private final Document doc;
    private ProvenanceEventRecord fullRecord;
    
    public LazyInitializedProvenanceEvent(final ProvenanceEventRepository repo, final StorageLocation storageLocation, final Document document) {
        this.repo = repo;
        this.storageLocation = storageLocation;
        this.doc = document;
    }

    @Override
    public long getEventId() {
        return doc.getField(IndexedFieldNames.EVENT_ID).numericValue().longValue();
    }
    
    @Override
    public StorageLocation getStorageLocation() {
        return storageLocation;
    }

    @Override
    public long getEventTime() {
        return doc.getField(SearchableFields.EventTime.getSearchableFieldName()).numericValue().longValue();
    }

    private void ensureFullyLoaded() {
        if ( fullRecord != null ) {
            return;
        }
        
        final long id = getEventId();
        try {
            fullRecord = repo.getEvent(id);
        } catch (final IOException ioe) {
            final String containerName = doc.get(IndexedFieldNames.CONTAINER_NAME);
            final String sectionName = doc.get(IndexedFieldNames.SECTION_NAME);
            final String journalId = doc.get(IndexedFieldNames.JOURNAL_ID);
            
            final String error = "Failed to load event with ID " + id + " from container '" + containerName + "', section '" + sectionName + "', journal '" + journalId + "' due to " + ioe;
            logger.error(error);
            if ( logger.isDebugEnabled() ) {
                logger.error("", ioe);
            }

            throw new EventNotFoundException(error);
        }
    }
    
    @Override
    public long getFlowFileEntryDate() {
        ensureFullyLoaded();
        return fullRecord.getFlowFileEntryDate();
    }

    @Override
    public long getLineageStartDate() {
        final IndexableField field = doc.getField(SearchableFields.LineageStartDate.getSearchableFieldName());
        if ( field != null ) {
            return field.numericValue().longValue();
        }

        ensureFullyLoaded();
        return fullRecord.getLineageStartDate();
    }

    @Override
    public Set<String> getLineageIdentifiers() {
        ensureFullyLoaded();
        return fullRecord.getLineageIdentifiers();
    }

    @Override
    public long getFileSize() {
        final IndexableField field = doc.getField(SearchableFields.FileSize.getSearchableFieldName());
        if ( field != null ) {
            return field.numericValue().longValue();
        }

        ensureFullyLoaded();
        return fullRecord.getFileSize();
    }

    @Override
    public Long getPreviousFileSize() {
        ensureFullyLoaded();
        return fullRecord.getPreviousFileSize();
    }

    @Override
    public long getEventDuration() {
        // TODO: Allow Event Duration to be indexed; it could be interesting for reporting.
        ensureFullyLoaded();
        return fullRecord.getEventDuration();
    }

    @Override
    public ProvenanceEventType getEventType() {
        final String name = doc.get(SearchableFields.EventType.getSearchableFieldName());
        return ProvenanceEventType.valueOf(name.toUpperCase());
    }

    @Override
    public Map<String, String> getAttributes() {
        ensureFullyLoaded();
        return fullRecord.getAttributes();
    }

    @Override
    public String getAttribute(final String attributeName) {
        final String attr = doc.get(attributeName);
        if ( attr == null ) {
            ensureFullyLoaded();
            return fullRecord.getAttribute(attributeName);
        } else {
            return attr;
        }
    }

    @Override
    public Map<String, String> getPreviousAttributes() {
        ensureFullyLoaded();
        return fullRecord.getPreviousAttributes();
    }

    @Override
    public Map<String, String> getUpdatedAttributes() {
        ensureFullyLoaded();
        return fullRecord.getUpdatedAttributes();
    }

    @Override
    public String getComponentId() {
        final String componentId = doc.get(SearchableFields.ComponentID.getSearchableFieldName());
        if ( componentId == null ) {
            ensureFullyLoaded();
            return fullRecord.getComponentId();
        } else {
            return componentId;
        }
    }

    @Override
    public String getComponentType() {
        // TODO: Make indexable.
        ensureFullyLoaded();
        return fullRecord.getComponentType();
    }

    @Override
    public String getTransitUri() {
        final String transitUri = doc.get(SearchableFields.TransitURI.getSearchableFieldName());
        if ( transitUri == null ) {
            final ProvenanceEventType eventType = getEventType();
            switch (eventType) {
                case RECEIVE:
                case SEND:
                    ensureFullyLoaded();
                    return fullRecord.getTransitUri();
                default:
                    return null;
            }
        } else {
            return transitUri;
        }
    }

    @Override
    public String getSourceSystemFlowFileIdentifier() {
        ensureFullyLoaded();
        return fullRecord.getSourceSystemFlowFileIdentifier();
    }

    @Override
    public String getFlowFileUuid() {
        String uuid = doc.get(SearchableFields.FlowFileUUID.getSearchableFieldName());
        if ( uuid != null ) {
            return uuid;
        }

        ensureFullyLoaded();
        return fullRecord.getFlowFileUuid();
    }

    @Override
    public List<String> getParentUuids() {
        final IndexableField[] uuids = doc.getFields(SearchableFields.FlowFileUUID.getSearchableFieldName());
        if ( uuids == null || uuids.length < 2 ) {
            return Collections.emptyList();
        }
        
        switch (getEventType()) {
            case JOIN: {
                final List<String> parentUuids = new ArrayList<>(uuids.length - 1);
                for (int i=1; i < uuids.length; i++) {
                    parentUuids.add(uuids[i].stringValue());
                }
                return parentUuids;
            }
            default:
                return Collections.emptyList();
        }
    }

    @Override
    public List<String> getChildUuids() {
        final IndexableField[] uuids = doc.getFields(SearchableFields.FlowFileUUID.getSearchableFieldName());
        if ( uuids == null || uuids.length < 2 ) {
            return Collections.emptyList();
        }
        
        switch (getEventType()) {
            case REPLAY:
            case CLONE:
            case FORK: {
                final List<String> childUuids = new ArrayList<>(uuids.length - 1);
                for (int i=1; i < uuids.length; i++) {
                    childUuids.add(uuids[i].stringValue());
                }
                return childUuids;
            }
            default:
                return Collections.emptyList();
        }
    }

    @Override
    public String getAlternateIdentifierUri() {
        final String altId = doc.get(SearchableFields.AlternateIdentifierURI.getSearchableFieldName());
        if ( altId == null && getEventType() == ProvenanceEventType.ADDINFO ) {
            ensureFullyLoaded();
            return fullRecord.getAlternateIdentifierUri();
        } else { 
            return null;
        }
    }

    @Override
    public String getDetails() {
        final String details = doc.get(SearchableFields.Details.getSearchableFieldName());
        if ( details == null ) {
            ensureFullyLoaded();
            return fullRecord.getDetails();
        }
        return null;
    }

    @Override
    public String getRelationship() {
        final String relationship = doc.get(SearchableFields.Relationship.getSearchableFieldName());
        if ( relationship == null ) {
            ensureFullyLoaded();
            return fullRecord.getRelationship();
        }
        return null;
    }

    @Override
    public String getSourceQueueIdentifier() {
        final String queueId = doc.get(SearchableFields.SourceQueueIdentifier.getSearchableFieldName());
        if ( queueId == null ) {
            ensureFullyLoaded();
            return fullRecord.getSourceQueueIdentifier();
        }
        return null;
    }

    @Override
    public String getContentClaimSection() {
        final String claimSection = doc.get(SearchableFields.ContentClaimSection.getSearchableFieldName());
        if ( claimSection == null ) {
            ensureFullyLoaded();
            return fullRecord.getContentClaimSection();
        }
        return null;
    }

    @Override
    public String getPreviousContentClaimSection() {
        ensureFullyLoaded();
        return fullRecord.getPreviousContentClaimSection();
    }

    @Override
    public String getContentClaimContainer() {
        final String claimContainer = doc.get(SearchableFields.ContentClaimContainer.getSearchableFieldName());
        if ( claimContainer == null ) {
            ensureFullyLoaded();
            return fullRecord.getContentClaimContainer();
        }
        return null;
    }

    @Override
    public String getPreviousContentClaimContainer() {
        ensureFullyLoaded();
        return fullRecord.getPreviousContentClaimContainer();
    }

    @Override
    public String getContentClaimIdentifier() {
        final String claimIdentifier = doc.get(SearchableFields.ContentClaimIdentifier.getSearchableFieldName());
        if ( claimIdentifier == null ) {
            ensureFullyLoaded();
            return fullRecord.getContentClaimIdentifier();
        }
        return null;
    }

    @Override
    public String getPreviousContentClaimIdentifier() {
        ensureFullyLoaded();
        return fullRecord.getPreviousContentClaimIdentifier();
    }

    @Override
    public Long getContentClaimOffset() {
        final String claimOffset = doc.get(SearchableFields.ContentClaimOffset.getSearchableFieldName());
        if ( claimOffset == null ) {
            ensureFullyLoaded();
            return fullRecord.getContentClaimOffset();
        }
        return null;
    }

    @Override
    public Long getPreviousContentClaimOffset() {
        ensureFullyLoaded();
        return fullRecord.getPreviousContentClaimOffset();
    }

}
