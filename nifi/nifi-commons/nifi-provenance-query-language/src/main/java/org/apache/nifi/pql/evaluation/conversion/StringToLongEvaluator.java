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
package org.apache.nifi.pql.evaluation.conversion;

import org.apache.nifi.pql.evaluation.OperandEvaluator;
import org.apache.nifi.provenance.ProvenanceEventRecord;

public class StringToLongEvaluator implements OperandEvaluator<Long> {
    private final OperandEvaluator<String> stringEvaluator;
    
    public StringToLongEvaluator(final OperandEvaluator<String> stringEvaluator) {
        this.stringEvaluator = stringEvaluator;
    }
    
    @Override
    public Long evaluate(final ProvenanceEventRecord record) {
        final String result = stringEvaluator.evaluate(record);
        if (result == null) {
            return null;
        }

        final String trimmed = result.trim();
        if ( trimmed.isEmpty() ) {
            return 0L;
        }
        
        if ( isNumber(trimmed) ) {
            return Long.parseLong(trimmed);
        }
        
        return null;
    }
    
    private boolean isNumber(final String value) {
        for (int i=0; i < value.length(); i++) {
            final char c = value.charAt(i);
            if ( c < '0' || c > '9' ) {
                return false;
            }
        }
        
        return true;
    }

    @Override
    public int getEvaluatorType() {
        return -1;
    }

    @Override
    public Class<Long> getType() {
        return Long.class;
    }
}
