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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.pql.ProvenanceQuery;
import org.apache.nifi.pql.exception.ProvenanceQueryLanguageParsingException;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.provenance.query.ProvenanceQuerySubmission;
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
    
    
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(QUERY);
        properties.add(DESTINATION_FILE);
        return properties;
    }
    
    
    @Override
    public void onTrigger(final ReportingContext context) {
        try {
            final long startNanos = System.nanoTime();
            final ProvenanceQuerySubmission submission = context.getEventAccess().getProvenanceRepository().submitQuery(context.getProperty(QUERY).getValue());
            final ProvenanceResultSet rs = submission.getResult().getResultSet();
            final List<String> labels = rs.getLabels();
            
            int length = 2;
            for ( final String label : labels ) {
                length += label.length() + 2;
            }
            
            final StringBuilder sb = new StringBuilder("\n");
            for (int i=0; i < length; i++) {
                sb.append("-");
            }
            sb.append("\n| ");
            
            for ( final String label : labels ) {
                sb.append(label).append(" |");
            }
            sb.append("\n");
            
            for (int i=0; i < length; i++) {
                sb.append("-");
            }
            sb.append("\n");
            
            int rowCount = 0;
            while (rs.hasNext()) {
                final List<?> cols = rs.next();
                for ( final Object col : cols ) {
                    sb.append("| ").append(col);
                }
                sb.append(" |\n");
                rowCount++;
            }
            
            final String filename = context.getProperty(DESTINATION_FILE).getValue();
            if ( filename == null ) {
                getLogger().info(sb.toString());
            } else {
                final File file = new File(filename);
                final File directory = file.getParentFile();
                if ( !directory.exists() && !directory.mkdirs() ) {
                    throw new ProcessException("Cannot create directory " + directory + " to write to file");
                }
                
                try (final OutputStream fos = new FileOutputStream(file)) {
                    fos.write(sb.toString().getBytes(StandardCharsets.UTF_8));
                }
            }

            final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            getLogger().info("Successfully generated report with {} rows in the result; report generation took {} millis", new Object[] {rowCount, millis});
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
