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
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.controller.Availability;

import org.apache.nifi.controller.exception.ValidationException;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.dao.ControllerServiceDAO;

public class StandardControllerServiceDAO extends ComponentDAO implements ControllerServiceDAO {

    private ControllerServiceProvider serviceProvider;

    /**
     * Locates the specified controller service.
     *
     * @param controllerServiceId
     * @return
     */
    private ControllerServiceNode locateControllerService(final String controllerServiceId) {
        // get the controller service
        final ControllerServiceNode controllerService = serviceProvider.getControllerServiceNode(controllerServiceId);

        // ensure the controller service exists
        if (controllerService == null) {
            throw new ResourceNotFoundException(String.format("Unable to locate controller service with id '%s'.", controllerServiceId));
        }

        return controllerService;
    }

    /**
     * Creates a controller service.
     *
     * @param controllerServiceDTO The controller service DTO
     * @return The controller service
     */
    @Override
    public ControllerServiceNode createControllerService(final ControllerServiceDTO controllerServiceDTO) {
        // create the controller service
    	final Availability availability = Availability.valueOf(controllerServiceDTO.getAvailability().toUpperCase());
        final ControllerServiceNode controllerService = serviceProvider.createControllerService(controllerServiceDTO.getType(), availability, true);
        
        // ensure we can perform the update 
        verifyUpdate(controllerService, controllerServiceDTO);
        
        // perform the update
        configureControllerService(controllerService, controllerServiceDTO);
        
        return controllerService;
    }

    /**
     * Gets the specified controller service.
     *
     * @param controllerServiceId The controller service id
     * @return The controller service
     */
    @Override
    public ControllerServiceNode getControllerService(final String controllerServiceId) {
        return locateControllerService(controllerServiceId);
    }

    /**
     * Determines if the specified controller service exists.
     *
     * @param controllerServiceId
     * @return
     */
    @Override
    public boolean hasControllerService(final String controllerServiceId) {
        return serviceProvider.getControllerServiceNode(controllerServiceId) != null;
    }

    /**
     * Gets all of the controller services.
     *
     * @return The controller services
     */
    @Override
    public Set<ControllerServiceNode> getControllerServices() {
        return serviceProvider.getAllControllerServices();
    }

    /**
     * Updates the specified controller service.
     *
     * @param controllerServiceDTO The controller service DTO
     * @return The controller service
     */
    @Override
    public ControllerServiceNode updateControllerService(final ControllerServiceDTO controllerServiceDTO) {
        // get the controller service
        final ControllerServiceNode controllerService = locateControllerService(controllerServiceDTO.getId());
        
        // ensure we can perform the update 
        verifyUpdate(controllerService, controllerServiceDTO);
        
        // perform the update
        configureControllerService(controllerService, controllerServiceDTO);

        // enable or disable as appropriate
        if (isNotNull(controllerServiceDTO.getEnabled())) {
            final boolean proposedDisabled = !controllerServiceDTO.getEnabled();
            
            if (proposedDisabled != controllerService.isDisabled()) {
                if (proposedDisabled) {
                    serviceProvider.disableControllerService(controllerService);
                } else {
                    serviceProvider.enableControllerService(controllerService);
                }
            }
        }
        
        return controllerService;
    }

    /**
     * Validates the specified configuration for the specified controller service.
     * 
     * @param controllerService
     * @param controllerServiceDTO
     * @return 
     */
    private List<String> validateProposedConfiguration(final ControllerServiceNode controllerService, final ControllerServiceDTO controllerServiceDTO) {
        final List<String> validationErrors = new ArrayList<>();
        
        if (isNotNull(controllerServiceDTO.getAvailability())) {
            try {
                Availability.valueOf(controllerServiceDTO.getAvailability());
            } catch (IllegalArgumentException iae) {
                validationErrors.add(String.format("Availability: Value must be one of [%s]", StringUtils.join(Availability.values(), ", ")));
            }
        }
        
        return validationErrors;
    }
    
    @Override
    public void verifyDelete(final String controllerServiceId) {
        final ControllerServiceNode controllerService = locateControllerService(controllerServiceId);
        controllerService.verifyCanDelete();
    }

    @Override
    public void verifyUpdate(final ControllerServiceDTO controllerServiceDTO) {
        final ControllerServiceNode controllerService = locateControllerService(controllerServiceDTO.getId());
        verifyUpdate(controllerService, controllerServiceDTO);
    }
    
    /**
     * Verifies the controller service can be updated.
     * 
     * @param controllerService
     * @param controllerServiceDTO 
     */
    private void verifyUpdate(final ControllerServiceNode controllerService, final ControllerServiceDTO controllerServiceDTO) {
        if (isNotNull(controllerServiceDTO.getEnabled())) {
            if (controllerServiceDTO.getEnabled()) {
                controllerService.verifyCanEnable();
            } else {
                controllerService.verifyCanDisable();
            }
        }
        
        boolean modificationRequest = false;
        if (isAnyNotNull(controllerServiceDTO.getName(),
                controllerServiceDTO.getAvailability(),
                controllerServiceDTO.getAnnotationData(),
                controllerServiceDTO.getComments(),
                controllerServiceDTO.getProperties())) {
            modificationRequest = true;
            
            // validate the request
            final List<String> requestValidation = validateProposedConfiguration(controllerService, controllerServiceDTO);

            // ensure there was no validation errors
            if (!requestValidation.isEmpty()) {
                throw new ValidationException(requestValidation);
            }
        }
        
        if (modificationRequest) {
            controllerService.verifyCanUpdate();
        }
    }
    
    /**
     * Configures the specified controller service.
     * 
     * @param controllerService
     * @param controllerServiceDTO 
     */
    private void configureControllerService(final ControllerServiceNode controllerService, final ControllerServiceDTO controllerServiceDTO) {
        final String name = controllerServiceDTO.getName();
        final String annotationData = controllerServiceDTO.getAnnotationData();
        final String comments = controllerServiceDTO.getComments();
        final Map<String, String> properties = controllerServiceDTO.getProperties();
        
        if (isNotNull(name)) {
            controllerService.setName(name);
        }
        if (isNotNull(annotationData)) {
            controllerService.setAnnotationData(annotationData);
        }
        if (isNotNull(comments)) {
            controllerService.setComments(comments);
        }
        if (isNotNull(properties)) {
            for (final Map.Entry<String, String> entry : properties.entrySet()) {
                final String propName = entry.getKey();
                final String propVal = entry.getValue();
                if (isNotNull(propName) && propVal == null) {
                    controllerService.removeProperty(propName);
                } else if (isNotNull(propName)) {
                    controllerService.setProperty(propName, propVal);
                }
            }
        }
    }
    
    /**
     * Deletes the specified controller service.
     *
     * @param controllerServiceId The controller service id
     */
    @Override
    public void deleteControllerService(String controllerServiceId) {
        final ControllerServiceNode controllerService = locateControllerService(controllerServiceId);
        serviceProvider.removeControllerService(controllerService);
    }

    /* setters */
    public void setServiceProvider(ControllerServiceProvider serviceProvider) {
        this.serviceProvider = serviceProvider;
    }
}
