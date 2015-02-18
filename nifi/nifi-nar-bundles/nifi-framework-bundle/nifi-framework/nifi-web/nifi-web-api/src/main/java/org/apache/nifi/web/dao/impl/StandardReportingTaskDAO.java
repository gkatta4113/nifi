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
package org.apache.nifi.web.dao.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ReportingTaskNode;

import org.apache.nifi.controller.exception.ValidationException;
import org.apache.nifi.controller.reporting.ReportingTaskInstantiationException;
import org.apache.nifi.web.NiFiCoreException;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.ReportingTaskDTO;
import org.apache.nifi.web.dao.ReportingTaskDAO;

public class StandardReportingTaskDAO extends ComponentDAO implements ReportingTaskDAO {

    private FlowController flowController;

    /**
     * Locates the specified reporting task.
     *
     * @param reportingTaskId
     * @return
     */
    private ReportingTaskNode locateReportingTask(final String reportingTaskId) {
        // get the reporting task
        final ReportingTaskNode reportingTask = flowController.getReportingTaskNode(reportingTaskId);

        // ensure the reporting task exists
        if (reportingTask == null) {
            throw new ResourceNotFoundException(String.format("Unable to locate reporting task with id '%s'.", reportingTaskId));
        }

        return reportingTask;
    }

    /**
     * Creates a reporting task.
     *
     * @param reportingTaskDTO The reporting task DTO
     * @return The reporting task
     */
    @Override
    public ReportingTaskNode createReportingTask(final ReportingTaskDTO reportingTaskDTO) {
        try {
            // create the reporting task
            final ReportingTaskNode reportingTask = flowController.createReportingTask(reportingTaskDTO.getType(), reportingTaskDTO.getId(), true);

            // ensure we can perform the update 
            verifyUpdate(reportingTask, reportingTaskDTO);

            // perform the update
            configureReportingTask(reportingTask, reportingTaskDTO);

            return reportingTask;
        } catch (ReportingTaskInstantiationException rtie) {
            throw new NiFiCoreException(rtie.getMessage(), rtie);
        }
    }

    /**
     * Gets the specified reporting task.
     *
     * @param reportingTaskId The reporting task id
     * @return The reporting task
     */
    @Override
    public ReportingTaskNode getReportingTask(final String reportingTaskId) {
        return locateReportingTask(reportingTaskId);
    }

    /**
     * Determines if the specified reporting task exists.
     *
     * @param reportingTaskId
     * @return
     */
    @Override
    public boolean hasReportingTask(final String reportingTaskId) {
        return flowController.getReportingTaskNode(reportingTaskId) != null;
    }

    /**
     * Gets all of the reporting tasks.
     *
     * @return The reporting tasks
     */
    @Override
    public Set<ReportingTaskNode> getReportingTasks() {
        return null;
    }

    /**
     * Updates the specified reporting task.
     *
     * @param reportingTaskDTO The reporting task DTO
     * @return The reporting task
     */
    @Override
    public ReportingTaskNode updateReportingTask(final ReportingTaskDTO reportingTaskDTO) {
        // get the reporting task
        final ReportingTaskNode controllerService = locateReportingTask(reportingTaskDTO.getId());
        
        // ensure we can perform the update 
        verifyUpdate(controllerService, reportingTaskDTO);
        
        // perform the update
        configureReportingTask(controllerService, reportingTaskDTO);

        // configure scheduled state
        
        return controllerService;
    }

    /**
     * Validates the specified configuration for the specified reporting task.
     * 
     * @param reportingTask
     * @param reportingTaskDTO
     * @return 
     */
    private List<String> validateProposedConfiguration(final ReportingTaskNode reportingTask, final ReportingTaskDTO reportingTaskDTO) {
        final List<String> validationErrors = new ArrayList<>();
        return validationErrors;
    }
    
    @Override
    public void verifyDelete(final String reportingTaskId) {
        final ReportingTaskNode reportingTask = locateReportingTask(reportingTaskId);
        reportingTask.verifyCanDelete();
    }

    @Override
    public void verifyUpdate(final ReportingTaskDTO reportingTaskDTO) {
        final ReportingTaskNode reportingTask = locateReportingTask(reportingTaskDTO.getId());
        verifyUpdate(reportingTask, reportingTaskDTO);
    }
    
    /**
     * Verifies the reporting task can be updated.
     * 
     * @param reportingTask
     * @param reportingTaskDTO 
     */
    private void verifyUpdate(final ReportingTaskNode reportingTask, final ReportingTaskDTO reportingTaskDTO) {
        boolean modificationRequest = false;
        if (isAnyNotNull(reportingTaskDTO.getName(),
                reportingTaskDTO.getAnnotationData(),
                reportingTaskDTO.getProperties())) {
            modificationRequest = true;
            
            // validate the request
            final List<String> requestValidation = validateProposedConfiguration(reportingTask, reportingTaskDTO);

            // ensure there was no validation errors
            if (!requestValidation.isEmpty()) {
                throw new ValidationException(requestValidation);
            }
        }
        
        if (modificationRequest) {
            reportingTask.verifyCanUpdate();
        }
    }
    
    /**
     * Configures the specified reporting task.
     * 
     * @param reportingTask
     * @param reportingTaskDTO 
     */
    private void configureReportingTask(final ReportingTaskNode reportingTask, final ReportingTaskDTO reportingTaskDTO) {
        final String name = reportingTaskDTO.getName();
        final String annotationData = reportingTaskDTO.getAnnotationData();
        final Map<String, String> properties = reportingTaskDTO.getProperties();
        
        if (isNotNull(name)) {
            reportingTask.setName(name);
        }
        if (isNotNull(annotationData)) {
            reportingTask.setAnnotationData(annotationData);
        }
        if (isNotNull(properties)) {
            for (final Map.Entry<String, String> entry : properties.entrySet()) {
                final String propName = entry.getKey();
                final String propVal = entry.getValue();
                if (isNotNull(propName) && propVal == null) {
                    reportingTask.removeProperty(propName);
                } else if (isNotNull(propName)) {
                    reportingTask.setProperty(propName, propVal);
                }
            }
        }
    }
    
    /**
     * Deletes the specified reporting task.
     *
     * @param reportingTaskId The reporting task id
     */
    @Override
    public void deleteReportingTask(String reportingTaskId) {
        final ReportingTaskNode reportingTask = locateReportingTask(reportingTaskId);
        flowController.removeReportingTask(reportingTask);
    }

    /* setters */
    
    public void setFlowController(FlowController flowController) {
        this.flowController = flowController;
    }
}
