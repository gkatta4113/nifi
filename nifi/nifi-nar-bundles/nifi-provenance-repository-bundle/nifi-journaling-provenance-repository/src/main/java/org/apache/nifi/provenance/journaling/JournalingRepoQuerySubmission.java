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

import java.util.Date;
import java.util.UUID;

import org.apache.nifi.provenance.query.ProvenanceQueryResult;
import org.apache.nifi.provenance.query.ProvenanceQuerySubmission;

public class JournalingRepoQuerySubmission implements ProvenanceQuerySubmission {

    private final Date submissionTime = new Date();
    private final String identifier = UUID.randomUUID().toString();
    private final ProvenanceQueryResult result;
    
    private final String query;
    private final Runnable cancelCallback;
    
    private volatile boolean canceled = false;
    
    public JournalingRepoQuerySubmission(final String query, final ProvenanceQueryResult queryResult) {
        this(query, queryResult, null);
    }
    
    public JournalingRepoQuerySubmission(final String query, final ProvenanceQueryResult queryResult, final Runnable cancelCallback) {
        this.query = query;
        this.cancelCallback = cancelCallback;
        this.result = queryResult;
    }
    
    @Override
    public String getQuery() {
        return query;
    }

    @Override
    public ProvenanceQueryResult getResult() {
        return result;
    }

    @Override
    public Date getSubmissionTime() {
        return submissionTime;
    }

    @Override
    public String getQueryIdentifier() {
        return identifier;
    }

    @Override
    public void cancel() {
        this.canceled = true;
        
        if ( cancelCallback != null ) {
            cancelCallback.run();
        }
    }

    @Override
    public boolean isCanceled() {
        return canceled;
    }

}
