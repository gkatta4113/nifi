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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.dao.ControllerServiceDAO;

public class StandardControllerServiceDAO extends ComponentDAO implements ControllerServiceDAO {

    private FlowController flowController;

    /**
     * Locates the specified controller service.
     *
     * @param controllerServiceId
     * @return
     */
    private ControllerServiceNode locateControllerService(final String controllerServiceId) {
        // get the controller service
        final ControllerServiceNode controllerService = flowController.getControllerServiceNode(controllerServiceId);

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
        final Map<String, String> temp = new HashMap<>();
        
        // create the controller service
        final ControllerServiceNode controllerService = flowController.createControllerService(controllerServiceDTO.getType(), controllerServiceDTO.getName(), temp);
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
        return flowController.getControllerServiceNode(controllerServiceId) != null;
    }

    /**
     * Gets all of the controller services.
     *
     * @return The controller services
     */
    @Override
    public Set<ControllerServiceNode> getControllerServices() {
        return null;
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
        
        return controllerService;
    }

    @Override
    public void verifyDelete(final String controllerServiceId) {
        final ControllerServiceNode controllerService = locateControllerService(controllerServiceId);
//        controllerService.verifyCanDelete();
    }

    @Override
    public void verifyUpdate(final ControllerServiceDTO controllerServiceDTO) {
        final ControllerServiceNode controllerService = locateControllerService(controllerServiceDTO.getId());
//        controllerService.verifyCanDelete();
    }
    
    /**
     * Deletes the specified controller service.
     *
     * @param controllerServiceId The controller service id
     */
    @Override
    public void deleteControllerService(String controllerServiceId) {
        final ControllerServiceNode controllerService = locateControllerService(controllerServiceId);
    }

    /* setters */
    public void setFlowController(FlowController flowController) {
        this.flowController = flowController;
    }
}
