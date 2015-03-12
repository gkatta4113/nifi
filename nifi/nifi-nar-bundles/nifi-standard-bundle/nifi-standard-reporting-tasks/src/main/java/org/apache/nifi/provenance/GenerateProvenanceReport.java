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
package org.apache.nifi.provenance;

import java.io.IOException;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.pql.ProvenanceQuery;
import org.apache.nifi.pql.exception.ProvenanceQueryLanguageParsingException;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.provenance.query.ProvenanceResultSet;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.ReportingContext;

public class GenerateProvenanceReport extends AbstractReportingTask {

    public static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
        .name("Provenance Query")
        .description("The Provenance Query to run against the repository")
        .required(true)
        .expressionLanguageSupported(false)
        .addValidator(new ProvenanceQueryLanguageValidator())
        .build();
    public static final PropertyDescriptor DESTINATION_FILE = new PropertyDescriptor.Builder()
        .name("Destination File")
        .description("The file to write the results to. If not specified, the results will be written to the log")
        .required(false)
        .expressionLanguageSupported(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();
    
    
    private ProvenanceQuery query;
    
    @OnScheduled
    public synchronized void compileQuery(final ReportingContext context) {
        final String queryText = context.getProperty(QUERY).getValue();
        this.query = ProvenanceQuery.compile(queryText, context.getEventAccess().getProvenanceRepository().getSearchableFields(), 
                context.getEventAccess().getProvenanceRepository().getSearchableAttributes());
    }
    
    @Override
    public synchronized void onTrigger(final ReportingContext context) {
        try {
            final ProvenanceResultSet rs = query.execute(context.getEventAccess().getProvenanceRepository());
            
        } catch (final IOException ioe) {
            throw new ProcessException(ioe);
        }
    }

    private static class ProvenanceQueryLanguageValidator implements Validator {
        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            try {
                ProvenanceQuery.compile(input, null, null);
            } catch (final ProvenanceQueryLanguageParsingException e) {
                return new ValidationResult.Builder().input(input).subject(subject).valid(false).explanation(e.getMessage()).build();
            }
            
            return new ValidationResult.Builder().input(input).subject(subject).valid(true).build();
        }
    }
}
