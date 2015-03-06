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
package org.apache.nifi.controller.reporting;

import org.apache.nifi.controller.ReportingTaskNode;

/**
 * A ReportingTaskProvider is responsible for providing management of, and access to, Reporting Tasks
 */
public interface ReportingTaskProvider {

    /**
     * Creates a new instance of a reporting task
     * 
     * @param type the type (fully qualified class name) of the reporting task to instantiate
     * @param id the identifier for the Reporting Task
     * @param firstTimeAdded whether or not this is the first time that the reporting task is being added
     * to the flow. I.e., this will be true only when the user adds the reporting task to the flow, not when
     * the flow is being restored after a restart of the software
     * 
     * @return the ReportingTaskNode that is used to manage the reporting task
     * 
     * @throws ReportingTaskInstantiationException if unable to create the Reporting Task
     */
    ReportingTaskNode createReportingTask(String type, String id, boolean firstTimeAdded) throws ReportingTaskInstantiationException;
    
    /**
     * Returns the reporting task that has the given identifier, or <code>null</code> if no reporting task
     * exists with that ID.
     * 
     * @param identifier
     * @return
     */
    ReportingTaskNode getReportingTaskNode(String identifier);
    
    
    /**
     * Removes the given reporting task from the flow
     * 
     * @param reportingTask
     * 
     * @throws IllegalStateException if the reporting task cannot be removed because it is not stopped, or
     * if the reporting task is not known in the flow
     */
    void removeReportingTask(ReportingTaskNode reportingTask);
    
}
