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

/* global nf, d3 */

nf.ControllerService = (function () {

    var config = {
        serviceOnly: 'SERVICE_ONLY',
        serviceAndReferencingComponents: 'SERVICE_AND_REFERENCING_COMPONENTS'
    };

    /**
     * Handle any expected controller service configuration errors.
     * 
     * @argument {object} xhr       The XmlHttpRequest
     * @argument {string} status    The status of the request
     * @argument {string} error     The error
     */
    var handleControllerServiceConfigurationError = function (xhr, status, error) {
        if (xhr.status === 400) {
            var errors = xhr.responseText.split('\n');

            var content;
            if (errors.length === 1) {
                content = $('<span></span>').text(errors[0]);
            } else {
                content = nf.Common.formatUnorderedList(errors);
            }

            nf.Dialog.showOkDialog({
                dialogContent: content,
                overlayBackground: false,
                headerText: 'Configuration Error'
            });
        } else {
            nf.Common.handleAjaxError(xhr, status, error);
        }
    };

    /**
     * Determines whether the user has made any changes to the controller service configuration
     * that needs to be saved.
     */
    var isSaveRequired = function () {
        var details = $('#controller-service-configuration').data('controllerServiceDetails');

        // determine if any controller service settings have changed

        if ($('#controller-service-name').val() !== details.name) {
            return true;
        }
        if ($('#controller-service-comments').val() !== details.comments) {
            return true;
        }
        
        if ($('#controller-service-enabled').hasClass('checkbox-checked')) {
            return true;
        }
        
        // defer to the properties
        return $('#controller-service-properties').propertytable('isSaveRequired');
    };

    /**
     * Marshals the data that will be used to update the contr oller service's configuration.
     */
    var marshalDetails = function () {
        // properties
        var properties = $('#controller-service-properties').propertytable('marshalProperties');

        // create the controller service dto
        var controllerServiceDto = {};
        controllerServiceDto['id'] = $('#controller-service-id').text();
        controllerServiceDto['name'] = $('#controller-service-name').val();
        
        // set the properties
        if ($.isEmptyObject(properties) === false) {
            controllerServiceDto['properties'] = properties;
        }
        
        // mark the controller service enabled if appropriate
        if ($('#controller-service-enabled').hasClass('checkbox-checked')) {
            controllerServiceDto['state'] = 'ENABLED';
        }

        // create the controller service entity
        var controllerServiceEntity = {};
        controllerServiceEntity['revision'] = nf.Client.getRevision();
        controllerServiceEntity['controllerService'] = controllerServiceDto;

        // return the marshaled details
        return controllerServiceEntity;
    };

    /**
     * Validates the specified details.
     * 
     * @argument {object} details       The details to validate
     */
    var validateDetails = function (details) {
        return true;
    };
    
    /**
     * Reloads the specified controller service.
     * 
     * @param {object} controllerService
     */
    var reloadControllerService = function (controllerService) {
        return $.ajax({
            type: 'GET',
            url: controllerService.uri,
            dataType: 'json'
        }).done(function (response) {
            renderControllerService(response.controllerService);
        }).fail(nf.Common.handleAjaxError);
    };
    
    /**
     * Renders the specified controller service.
     * 
     * @param {object} controllerService
     */
    var renderControllerService = function (controllerService) {
        // get the table and update the row accordingly
        var controllerServiceGrid = $('#controller-services-table').data('gridInstance');
        var controllerServiceData = controllerServiceGrid.getData();
        controllerServiceData.updateItem(controllerService.id, controllerService);

        // reload the controller service referencing components
        reloadControllerServiceReferences(controllerService);
    };
    
    /**
     * Reloads components that reference this controller service as well as
     * other services that this controller service references.
     * 
     * @param {object} controllerService
     */
    var reloadControllerServiceReferences = function (controllerService) {
        var reloadOther = false;

        // reload all dependent processors if they are currently visible
        $.each(controllerService.referencingComponents, function (_, reference) {
            if (reference.referenceType === 'Processor') {
                // reload the processor on the canvas if appropriate
                if (nf.Canvas.getGroupId() === reference.groupId) {
                    var processor = nf.Processor.get(reference.id);
                    nf.Processor.reload(processor.component);
                }
                
                // update the current state of this processor
                var referencingComponentState = $('div.' + reference.id + '-state');
                if (referencingComponentState.length) {
                    updateReferencingSchedulableComponentState(referencingComponentState, reference);
                }
            } else if (reference.referenceType === 'ReportingTask') {
                reloadOther = true;
                
                // update the current state of this reporting task
                var referencingComponentState = $('div.' + reference.id + '-state');
                if (referencingComponentState.length) {
                    updateReferencingSchedulableComponentState(referencingComponentState, reference);
                }
            } else {
                reloadOther = true;
                
                // update the current state of this service
                var referencingComponentState = $('div.' + reference.id + '-state');
                if (referencingComponentState.length) {
                    updateReferencingServiceState(referencingComponentState, reference);
                }
            }
        });

        // see if this controller service references another controller service
        if (reloadOther === false) {
            $.each(controllerService.descriptors, function(_, descriptor) {
                if (descriptor.identifiesControllerService === true) {
                    reloadOther = true;
                    return false;
                }
            });
        }

        // reload the controller services and reporting tasks if necessary
        if (reloadOther) {
            nf.Settings.loadSettings();
        }
    };   
    
    /**
     * Adds a border to the controller service referencing components if necessary.
     * 
     * @argument {jQuery} referenceContainer 
     */
    var updateReferencingComponentsBorder = function (referenceContainer) {
        // determine if it is too big
        var tooBig = referenceContainer.get(0).scrollHeight > referenceContainer.innerHeight() ||
                referenceContainer.get(0).scrollWidth > referenceContainer.innerWidth();
        
        // draw the border if necessary
        if (referenceContainer.is(':visible') && tooBig) {
            referenceContainer.css('border-width', '1px');
        } else {
            referenceContainer.css('border-width', '0px');
        }
    };
    
    /**
     * Updates the referencingComponentState using the specified referencingComponent.
     * 
     * @param {jQuery} referencingComponentState
     * @param {object} referencingComponent
     */
    var updateReferencingSchedulableComponentState = function (referencingComponentState, referencingComponent) {
        referencingComponentState.removeClass('disabled stopped running invalid').addClass(function() {
            var icon = $(this);

            var state = referencingComponent.state.toLowerCase();
            if (state === 'stopped' && !nf.Common.isEmpty(referencingComponent.validationErrors)) {
                state = 'invalid';

                // add tooltip for the warnings
                var list = nf.Common.formatUnorderedList(referencingComponent.validationErrors);
                if (icon.data('qtip')) {
                    icon.qtip('option', 'content.text', list);
                } else {
                    icon.qtip($.extend({
                        content: list
                    }, nf.CanvasUtils.config.systemTooltipConfig));
                }
            } else if (icon.data('qtip')) {
                icon.qtip('destroy');
            }
            return state;
        });
    };
    
    /**
     * Updates the referencingServiceState using the specified referencingService.
     * 
     * @param {jQuery} referencingServiceState
     * @param {object} referencingService
     */
    var updateReferencingServiceState = function (referencingServiceState, referencingService) {
        referencingServiceState.removeClass('disabled enabled invalid').addClass(function() {
            var icon = $(this);

            var state = referencingService.state === 'ENABLED' ? 'enabled' : 'disabled';
            if (state === 'disabled' && !nf.Common.isEmpty(referencingService.validationErrors)) {
                state = 'invalid';

                // add tooltip for the warnings
                var list = nf.Common.formatUnorderedList(referencingService.validationErrors);
                if (icon.data('qtip')) {
                    icon.qtip('option', 'content.text', list);
                } else {
                    icon.qtip($.extend({
                        content: list
                    }, nf.CanvasUtils.config.systemTooltipConfig));
                }
            } else if (icon.data('qtip')) {
                icon.qtip('destroy');
            }
            return state;
        });
    };
    
    /**
     * Adds the specified reference for this controller service.
     * 
     * @argument {jQuery} referenceContainer 
     * @param {array} referencingComponents
     */
    var createReferencingComponents = function (referenceContainer, referencingComponents) {
        if (nf.Common.isEmpty(referencingComponents)) {
            referenceContainer.append('<div class="unset">No referencing components.</div>');
            return;
        }
        
        // toggles the visibility of a container
        var toggle = function (twist, container) {
            if (twist.hasClass('expanded')) {
                twist.removeClass('expanded').addClass('collapsed');
                container.hide();
            } else {
                twist.removeClass('collapsed').addClass('expanded');
                container.show();
            }
        };
        
        var processors = $('<ul class="referencing-component-listing clear"></ul>');
        var services = $('<ul class="referencing-component-listing clear"></ul>');
        var tasks = $('<ul class="referencing-component-listing clear"></ul>');
        $.each(referencingComponents, function (_, referencingComponent) {
            if (referencingComponent.referenceType === 'Processor') {
                var processorLink = $('<span class="referencing-component-name link"></span>').text(referencingComponent.name).on('click', function () {
                    // show the component
                    nf.CanvasUtils.showComponent(referencingComponent.groupId, referencingComponent.id);
                    
                    // close the dialog and shell
                    referenceContainer.closest('.dialog').modal('hide');
                    $('#shell-close-button').click();
                });

                // active thread count
                var activeThreadCount = $('<span class="referencing-component-active-thread-count"></span>').addClass(referencingComponent.id + '-active-threads');
                if (nf.Common.isDefinedAndNotNull(referencingComponent.activeThreadCount) && referencingComponent.activeThreadCount > 0) {
                    activeThreadCount.text('(' + referencingComponent.activeThreadCount + ')');
                }
                
                // state
                var processorState = $('<div class="referencing-component-state"></div>').addClass(referencingComponent.id + '-state');
                updateReferencingSchedulableComponentState(processorState, referencingComponent);
                
                // type
                var processorType = $('<span class="referencing-component-type"></span>').text(nf.Common.substringAfterLast(referencingComponent.type, '.'));
                
                // processor
                var processorItem = $('<li></li>').append(processorState).append(activeThreadCount).append(processorLink).append(processorType);
                processors.append(processorItem);
            } else if (referencingComponent.referenceType === 'ControllerService') {
                var serviceLink = $('<span class="referencing-component-name link"></span>').text(referencingComponent.name).on('click', function () {
                    var controllerServiceGrid = $('#controller-services-table').data('gridInstance');
                    var controllerServiceData = controllerServiceGrid.getData();
                    
                    // select the selected row
                    var row = controllerServiceData.getRowById(referencingComponent.id);
                    controllerServiceGrid.setSelectedRows([row]);
                    
                    // close the dialog and shell
                    referenceContainer.closest('.dialog').modal('hide');
                });
                
                // container for this service's references
                var referencingServiceReferencesContainer = $('<div class="referencing-component-references hidden"></div>');
                var serviceTwist = $('<span class="service expansion-button collapsed pointer"></span>').on('click', function() {
                    if (serviceTwist.hasClass('collapsed')) {
                        var controllerServiceGrid = $('#controller-services-table').data('gridInstance');
                        var controllerServiceData = controllerServiceGrid.getData();
                        var referencingService = controllerServiceData.getItemById(referencingComponent.id);
                        
                        // create the markup for the references
                        createReferencingComponents(referencingServiceReferencesContainer, referencingService.referencingComponents);
                    } else {
                        referencingServiceReferencesContainer.empty();
                    }
                    
                    // toggle visibility
                    toggle(serviceTwist, referencingServiceReferencesContainer);
                    
                    // update borders as necessary
                    updateReferencingComponentsBorder(referenceContainer);
                });
                
                // state
                var serviceState = $('<div class="referencing-component-state"></div>').addClass(referencingComponent.id + '-state');
                updateReferencingServiceState(serviceState, referencingComponent);
                
                // type
                var serviceType = $('<span class="referencing-component-type"></span>').text(nf.Common.substringAfterLast(referencingComponent.type, '.'));
                
                // service
                var serviceItem = $('<li></li>').append(serviceTwist).append(serviceState).append(serviceLink).append(serviceType).append(referencingServiceReferencesContainer);
                
                services.append(serviceItem);
            } else if (referencingComponent.referenceType === 'ReportingTask') {
                var taskItem = $('<li></li>').text(referencingComponent.name).on('click', function () {
                    
                    // close the dialog and shell
                    $('#controller-service-configuration').modal('hide');
                });
                tasks.append(taskItem);
            }
        });
        
        // create the collapsable listing for each type
        var createReferenceBlock = function (titleText, list) {
            if (list.is(':empty')) {
                list.remove();
                return;
            }
            
            var twist = $('<span class="expansion-button expanded"></span>');
            var title = $('<span class="referencing-component-title"></span>').text(titleText);
            var count = $('<span class="referencing-component-count"></span>').text('(' + list.children().length + ')');
            
            // create the reference block
            $('<div class="referencing-component-block pointer unselectable"></div>').on('click', function () {
                // toggle this block
                toggle(twist, list);
                
                // update the border if necessary
                updateReferencingComponentsBorder(referenceContainer);
            }).append(twist).append(title).append(count).appendTo(referenceContainer);
            
            // add the listing
            list.appendTo(referenceContainer);
        };
        
        // create blocks for each type of component
        createReferenceBlock('Processors', processors);
        createReferenceBlock('Reporting Tasks', tasks);
        createReferenceBlock('Controller Services', services);
    };
    
    /**
     * Sets whether the specified controller service is enabled.
     * 
     * @param {object} controllerService
     * @param {boolean} enabled
     * @param {function} pollCondition
     */
    var setEnabled = function (controllerService, enabled, pollCondition) {
        var revision = nf.Client.getRevision();
        
        var updated = $.ajax({
            type: 'PUT',
            url: controllerService.uri,
            data: {
                clientId: revision.clientId,
                version: revision.version,
                state: enabled === true ? 'ENABLED' : 'DISABLED'
            },
            dataType: 'json'
        }).done(function (response) {
            nf.Client.setRevision(response.revision);
        }).fail(nf.Common.handleAjaxError);
        
        // wait unil the polling of each service finished
        return $.Deferred(function(deferred) {
            updated.done(function(response) {
                var serviceUpdated = pollService(controllerService, function (service) {
                    // update the service in the table
                    renderControllerService(service);
                    
                    // the condition is met once the service is ENABLED/DISABLED
                    if (enabled) {
                        return service.state === 'ENABLED';
                    } else {
                        return service.state === 'DISABLED';
                    }
                }, pollCondition);

                // once the service has updated, resolve and render the updated service
                serviceUpdated.done(function () {
                    deferred.resolve();
                    
                    // update the service in the table
                    renderControllerService(response.controllerService);
                }).fail(function() {
                    deferred.reject();
                });
            }).fail(function() {
                deferred.reject();
            });
        }).promise();
    };
    
    /**
     * Gets the id's of all controller services referencing the specified controller service.
     * 
     * @param {object} controllerService
     */
    var getReferencingControllerServiceIds = function (controllerService) {
        var ids = d3.set();
        ids.add(controllerService.id);
        
        var checkReferencingServices = function (referencingComponents) {
            $.each(referencingComponents, function (_, referencingComponent) {
                if (referencingComponent.referenceType === 'ControllerService') {
                    // add the id
                    ids.add(referencingComponent.id);
                    
                    // consider it's referencing components if appropriate
                    if (referencingComponent.referenceCycle === false) {
                        checkReferencingServices(referencingComponent.referencingComponents);
                    }
                }
            });
        };

        // check the referencing servcies
        checkReferencingServices(controllerService.referencingComponents);
        return ids;
    };
    
    /**
     * Updates the scheduled state of the processors/reporting tasks referencing
     * the specified controller service.
     * 
     * @param {type} controllerService
     * @param {type} running
     * @param {type} pollCondition
     */
    var updateReferencingSchedulableComponents = function (controllerService, running, pollCondition) {
        var revision = nf.Client.getRevision();
        
        // issue the request to update the referencing components
        var updated = $.ajax({
            type: 'PUT',
            url: controllerService.uri + '/references',
            data: {
                clientId: revision.clientId,
                version: revision.version,
                state: running ? 'RUNNING' : 'STOPPED'
            },
            dataType: 'json'
        }).done(function (response) {
            nf.Client.setRevision(response.revision);
        }).fail(nf.Common.handleAjaxError);
        
        // wait unil the polling of each service finished
        return $.Deferred(function(deferred) {
            updated.done(function(response) {
                // update the controller service
                controllerService.referencingComponents = response.controllerServiceReferencingComponents;
                
                // if we're just starting schedulable components we're done when the update is finished
                if (running) {
                    deferred.resolve();
                    
                    // reload the controller service
                    reloadControllerServiceReferences(controllerService);
                } else {
                    // identify all referencing services
                    var services = getReferencingControllerServiceIds(controllerService);

                    // get the controller service grid
                    var controllerServiceGrid = $('#controller-services-table').data('gridInstance');
                    var controllerServiceData = controllerServiceGrid.getData();

                    // start polling for each controller service
                    var polling = [];
                    services.forEach(function(controllerServiceId) {
                        var controllerService = controllerServiceData.getItemById(controllerServiceId);
                        polling.push(stopReferencingSchedulableComponents(controllerService, pollCondition));
                    });

                }
                
                $.when.apply(window, polling).done(function () {
                    deferred.resolve();
                }).fail(function() {
                    deferred.reject();
                });
            }).fail(function() {
                deferred.reject();
            });
        }).promise();
    };
    
    /**
     * Polls the specified services referencing components to see if the
     * specified condition is satisfied.
     * 
     * @param {object} controllerService
     * @param {function} completeCondition
     * @param {function} pollCondition
     */
    var pollService = function (controllerService, completeCondition, pollCondition) {
        // we want to keep polling until the condition is met
        return $.Deferred(function(deferred) {
            var current = 1;
            var getTimeout = function () {
                var val = current;
                
                // update the current timeout for the next time
                current = Math.max(current * 2, 8);
                
                return val * 1000;
            };
            
            // polls for the current status of the referencing components
            var poll = function() {
                $.ajax({
                    type: 'GET',
                    url: controllerService.uri,
                    dataType: 'json'
                }).done(function (response) {
                    conditionMet(response.controllerService);
                }).fail(function (xhr, status, error) {
                    deferred.reject();
                    nf.Common.handleAjaxError(xhr, status, error);
                });
            };
            
            // tests to if the condition has been met
            var conditionMet = function (service) {
                if (completeCondition(service)) {
                    deferred.resolve();
                } else {
                    if (typeof pollCondition === 'function' && pollCondition()) {
                        setTimeout(poll(), getTimeout());
                    } else {
                        deferred.reject();
                    }
                }
            };
            
            // poll for the status of the referencing components
            conditionMet(controllerService);
        }).promise();
    };
    
    /**
     * Continues to poll the specified controller service until all referencing schedulable 
     * components are stopped (not scheduled and 0 active threads).
     * 
     * @param {object} controllerService
     * @param {function} pollCondition
     */
    var stopReferencingSchedulableComponents = function (controllerService, pollCondition) {
        // continue to poll the service until all schedulable components have stopped
        return pollService(controllerService, function (service) {
            var referencingComponents = service.referencingComponents;
            
            // update the service in the table
            renderControllerService(service);
            
            var stillRunning = false;
            $.each(referencingComponents, function(_, referencingComponent) {
                if (referencingComponent.referenceType === 'Processor' || referencingComponent.referenceType === 'ReportingTask') {
                    if (referencingComponent.state !== 'STOPPED' || referencingComponent.activeThreadCount > 0) {
                        stillRunning = true;
                    }
                    
                    // update the current active thread count
                    $('div.' + referencingComponent.id + '-active-threads').text(referencingComponent.activeThreadCount);
                    
                    // update the current state of this component
                    var referencingComponentState = $('div.' + referencingComponent.id + '-state');
                    updateReferencingSchedulableComponentState(referencingComponentState, referencingComponent);
                }
            });

            // condition is met once all referencing are not running
            return stillRunning === false;
        }, pollCondition);
    };
    
    /**
     * Continues to poll until all referencing services are enabled.
     * 
     * @param {type} controllerService
     * @param {type} pollCondition
     */
    var enableReferencingServices = function (controllerService, pollCondition) {
        // continue to poll the service until all referencing services are enabled
        return pollService(controllerService, function (service) {
            var referencingComponents = service.referencingComponents;
            
            // update the service in the table
            renderControllerService(service);
            
            var notEnabled = false;
            $.each(referencingComponents, function(_, referencingComponent) {
                if (referencingComponent.referenceType === 'ControllerService') {
                    if (referencingComponent.state !== 'ENABLED') {
                        notEnabled = true;
                    } 
                        
                    // update the state of the referencing service
                    var referencingServiceState = $('div.' + referencingComponent.id + '-state');
                    updateReferencingServiceState(referencingServiceState, referencingComponent);
                }
            });

            // condition is met once all referencing are not disabled
            return notEnabled === false;
        }, pollCondition);
    };
    
    /**
     * Continues to poll until all referencing services are disabled.
     * 
     * @param {type} controllerService
     * @param {type} pollCondition
     */
    var disableReferencingServices = function (controllerService, pollCondition) {
        // continue to poll the service until all referencing services are disabled
        return pollService(controllerService, function (service) {
            var referencingComponents = service.referencingComponents;
            
            // update the service in the table
            renderControllerService(service);
            
            var notDisabled = false;
            $.each(referencingComponents, function(_, referencingComponent) {
                if (referencingComponent.referenceType === 'ControllerService') {
                    if (referencingComponent.state !== 'DISABLED') {
                        notDisabled = true;
                    } 
                        
                    // update the state of the referencing service
                    var referencingServiceState = $('div.' + referencingComponent.id + '-state');
                    updateReferencingServiceState(referencingServiceState, referencingComponent);
                }
            });

            // condition is met once all referencing are not enabled
            return notDisabled === false;
        }, pollCondition);
    };
    
    /**
     * Updates the referencing services with the specified state.
     * 
     * @param {object} controllerService
     * @param {boolean} enabled
     * @param {function} pollCondition
     */
    var updateReferencingServices = function (controllerService, enabled, pollCondition) {
        var revision = nf.Client.getRevision();
        
        // issue the request to update the referencing components
        var updated = $.ajax({
            type: 'PUT',
            url: controllerService.uri + '/references',
            data: {
                clientId: revision.clientId,
                version: revision.version,
                state: enabled ? 'ENABLED' : 'DISABLED'
            },
            dataType: 'json'
        }).done(function (response) {
            nf.Client.setRevision(response.revision);
        }).fail(nf.Common.handleAjaxError);
        
        // need to wait until finished ENALBING or DISABLING?
        // wait unil the polling of each service finished
        return $.Deferred(function(deferred) {
            updated.done(function(response) {
                // update the controller service
                controllerService.referencingComponents = response.controllerServiceReferencingComponents;
                
                // identify all referencing services
                var services = getReferencingControllerServiceIds(controllerService);

                // get the controller service grid
                var controllerServiceGrid = $('#controller-services-table').data('gridInstance');
                var controllerServiceData = controllerServiceGrid.getData();

                // start polling for each controller service
                var polling = [];
                services.forEach(function(controllerServiceId) {
                    var controllerService = controllerServiceData.getItemById(controllerServiceId);
                    
                    if (enabled) {
                        polling.push(enableReferencingServices(controllerService, pollCondition));
                    } else {
                        polling.push(disableReferencingServices(controllerService, pollCondition));
                    }
                });

                $.when.apply(window, polling).done(function () {
                    deferred.resolve();
                }).fail(function() {
                    deferred.reject();
                });
            }).fail(function() {
                deferred.reject();
            });
        }).promise();
    };
    
    /**
     * Shows the dialog for disabling a controller service.
     * 
     * @argument {object} controllerService The controller service to disable
     */
    var showDisableControllerServiceDialog = function (controllerService) {
        // populate the disable controller service dialog
        $('#disable-controller-service-id').text(controllerService.id);
        $('#disable-controller-service-name').text(controllerService.name);
        
        // load the controller referencing components list
        var referencingComponentsContainer = $('#disable-controller-service-referencing-components');
        createReferencingComponents(referencingComponentsContainer, controllerService.referencingComponents);
        
        // show the dialog
        $('#disable-controller-service-dialog').modal('show');
        
        // update the border if necessary
        updateReferencingComponentsBorder(referencingComponentsContainer);
    };
    
    /**
     * Shows the dialog for enabling a controller service.
     * 
     * @param {object} controllerService
     */
    var showEnableControllerServiceDialog = function (controllerService) {
        // populate the disable controller service dialog
        $('#enable-controller-service-id').text(controllerService.id);
        $('#enable-controller-service-name').text(controllerService.name);
        
        // load the controller referencing components list
        var referencingComponentsContainer = $('#enable-controller-service-referencing-components');
        createReferencingComponents(referencingComponentsContainer, controllerService.referencingComponents);
        
        // show the dialog
        $('#enable-controller-service-dialog').modal('show');
        
        // update the border if necessary
        updateReferencingComponentsBorder(referencingComponentsContainer);
    };
    
    /**
     * Used to handle closing a modal dialog
     */
    var closeModal = function() {
        $(this).modal('hide');
    };
    
    /**
     * Handles the disable action of the disable controller service dialog.
     */
    var disableHandler = function() {
        var disableDialog = $(this);
        var canceled = false;
                            
        // only provide a close option
        disableDialog.modal('setButtonModel', [{
            buttonText: 'Cancel',
            handler: {
                click: function () {
                    canceled = true;
                }
            }
        }]);

        // show the progress
        $('#disable-controller-service-service-container').hide();
        $('#disable-controller-service-scope-container').hide();
        $('#disable-controller-service-progress-container').show();

        // get the controller service
        var controllerServiceId = $('#disable-controller-service-id').text();
        var controllerServiceGrid = $('#controller-services-table').data('gridInstance');
        var controllerServiceData = controllerServiceGrid.getData();
        var controllerService = controllerServiceData.getItemById(controllerServiceId);

        // whether or not to continue polling
        var continuePolling = function () {
            return canceled === false;
        };
        
        // sets the close button on the dialog
        var setCloseButton = function () {
            disableDialog.modal('setButtonModel', [{
                buttonText: 'Close',
                handler: {
                    click: closeModal
                }
            }]);
        };

        $('#disable-progress-label').text('Steps to disable ' + controllerService.name);
        var disableReferencingSchedulable = $('#disable-referencing-schedulable').addClass('ajax-loading');

        // stop all referencing schedulable components
        var stopped = updateReferencingSchedulableComponents(controllerService, false, continuePolling);

        // once everything has stopped
        stopped.done(function () {
            disableReferencingSchedulable.removeClass('ajax-loading').addClass('ajax-complete');
            var disableReferencingServices = $('#disable-referencing-services').addClass('ajax-loading');
            
            // disable all referencing services
            var disabled = updateReferencingServices(controllerService, false, continuePolling);

            // everything is disabled
            disabled.done(function () {
                disableReferencingServices.removeClass('ajax-loading').addClass('ajax-complete');
                var disableControllerService = $('#disable-controller-service').addClass('ajax-loading');
                
                // disable this service
                setEnabled(controllerService, false, continuePolling).done(function () {
                    disableControllerService.removeClass('ajax-loading').addClass('ajax-complete');
                }).fail(function () {
                    disableControllerService.removeClass('ajax-loading').addClass('ajax-error');
                }).always(function () {
                    setCloseButton();
                });
            }).fail(function () {
                disableReferencingServices.removeClass('ajax-loading').addClass('ajax-error');
                setCloseButton();
            });
        }).fail(function () {
            disableReferencingSchedulable.removeClass('ajax-loading').addClass('ajax-error');
            setCloseButton();
        });
    };
    
    /**
     * Handles the enable action of the enable controller service dialog.
     */
    var enableHandler = function() {
        var enableDialog = $(this);
        var canceled = false;
                            
        // only provide a close option
        enableDialog.modal('setButtonModel', [{
            buttonText: 'Cancel',
            handler: {
                click: function () {
                    canceled = true;
                }
            }
        }]);

        // determine if we want to also activate referencing components
        var scope = $('#enable-controller-service-scope').combo('getSelectedOption').value;
        if (scope === config.serviceOnly) {
            $('#enable-controller-service-progress li.referencing-component').hide();
        }

        // show the progress
        $('#enable-controller-service-service-container').hide();
        $('#enable-controller-service-scope-container').hide();
        $('#enable-controller-service-progress-container').show();

        // get the controller service
        var controllerServiceId = $('#enable-controller-service-id').text();
        var controllerServiceGrid = $('#controller-services-table').data('gridInstance');
        var controllerServiceData = controllerServiceGrid.getData();
        var controllerService = controllerServiceData.getItemById(controllerServiceId);

        // whether or not to continue polling
        var continuePolling = function () {
            return canceled === false;
        };

        // sets the button to close
        var setCloseButton = function () {
            enableDialog.modal('setButtonModel', [{
                buttonText: 'Close',
                handler: {
                    click: closeModal
                }
            }]);
        };

        $('#enable-progress-label').text('Steps to enable ' + controllerService.name);
        var enableControllerService = $('#enable-controller-service').addClass('ajax-loading');

        // enable this controller service
        var enabled = setEnabled(controllerService, true, continuePolling);

        if (scope === config.serviceAndReferencingComponents) {
            // once the service is enabled, activate all referencing components
            enabled.done(function() {
                enableControllerService.removeClass('ajax-loading').addClass('ajax-complete');
                var enableReferencingServices = $('#enable-referencing-services').addClass('ajax-loading');
                
                // enable the referencing services
                var servicesEnabled = updateReferencingServices(controllerService, true, continuePolling);

                // once all the referencing services are enbled
                servicesEnabled.done(function () {
                    enableReferencingServices.removeClass('ajax-loading').addClass('ajax-complete');
                    var enableReferencingSchedulable = $('#enable-referencing-schedulable').addClass('ajax-loading');
                
                    // start all referencing schedulable components
                    updateReferencingSchedulableComponents(controllerService, true, continuePolling).done(function() {
                        enableReferencingSchedulable.removeClass('ajax-loading').addClass('ajax-complete');
                    }).fail(function () {
                        enableReferencingSchedulable.removeClass('ajax-loading').addClass('ajax-error');
                    }).always(function () {
                        setCloseButton();
                    });
                }).fail(function () {
                    enableReferencingServices.removeClass('ajax-loading').addClass('ajax-error');
                    setCloseButton();
                });
            }).fail(function () {
                enableControllerService.removeClass('ajax-loading').addClass('ajax-error');
                setCloseButton();
            });
        } else {
            enabled.done(function() {
                enableControllerService.removeClass('ajax-loading').addClass('ajax-complete');
            }).fail(function () {
                enableControllerService.removeClass('ajax-loading').addClass('ajax-error');
            }).always(function () {
                setCloseButton();
            });
        }
    };
    
    return {
        /**
         * Initializes the controller service configuration dialog.
         */
        init: function () {
            // initialize the configuration dialog tabs
            $('#controller-service-configuration-tabs').tabbs({
                tabStyle: 'tab',
                selectedTabStyle: 'selected-tab',
                tabs: [{
                        name: 'Settings',
                        tabContentId: 'controller-service-standard-settings-tab-content'
                    }, {
                        name: 'Properties',
                        tabContentId: 'controller-service-properties-tab-content'
                    }, {
                        name: 'Comments',
                        tabContentId: 'controller-service-comments-tab-content'
                    }],
                select: function () {
                    // update the property table size in case this is the first time its rendered
                    if ($(this).text() === 'Properties') {
                        $('#controller-service-properties').propertytable('resetTableSize');
                    }

                    // close all fields currently being edited
                    $('#controller-service-properties').propertytable('saveRow');

                    // show the border around the processor relationships if necessary
                    var referenceContainer = $('#controller-service-referencing-components');
                    updateReferencingComponentsBorder(referenceContainer);
                }
            });
            
            // we clustered we need to show the controls for editing the availability
            if (nf.Canvas.isClustered()) {
                $('#availability-setting-container').show();
            }

            // initialize the conroller service configuration dialog
            $('#controller-service-configuration').modal({
                headerText: 'Configure Controller Service',
                overlayBackground: false,
                handler: {
                    close: function () {
                        // empty the referencing components list
                        var referencingComponents = $('#controller-service-referencing-components');
                        nf.Common.cleanUpTooltips(referencingComponents, 'div.referencing-component-state');
                        referencingComponents.css('border-width', '0').empty();
                        
                        // cancel any active edits
                        $('#controller-service-properties').propertytable('cancelEdit');

                        // clear the tables
                        $('#controller-service-properties').propertytable('clear');
                        
                        // removed the cached controller service details
                        $('#controller-service-configuration').removeData('controllerServiceDetails');
                    }
                }
            }).draggable({
                containment: 'parent',
                handle: '.dialog-header'
            });

            // initialize the property table
            $('#controller-service-properties').propertytable({
                readOnly: false,
                newPropertyDialogContainer: 'body'
            });
            
            // initialize the disable service dialog
            $('#disable-controller-service-dialog').modal({
                headerText: 'Disable Controller Service',
                overlayBackground: false,
                buttons: [{
                    buttonText: 'Disable',
                    handler: {
                        click: disableHandler
                    }
                }, {
                    buttonText: 'Cancel',
                    handler: {
                        click: closeModal
                    }
                }],
                handler: {
                    close: function() {
                        var disableDialog = $(this);
                        
                        // reset visibility
                        $('#disable-controller-service-service-container').show();
                        $('#disable-controller-service-scope-container').show();
                        $('#disable-controller-service-progress-container').hide();
                        
                        // clear the dialog
                        $('#disable-controller-service-id').text('');
                        $('#disable-controller-service-name').text('');
                        
                        // reset progress
                        $('div.disable-referencing-components').removeClass('ajax-loading ajax-complete ajax-error');
                        
                        // referencing components
                        var referencingComponents = $('#disable-controller-service-referencing-components');
                        nf.Common.cleanUpTooltips(referencingComponents, 'div.referencing-component-state');
                        referencingComponents.css('border-width', '0').empty();
                        
                        // reset dialog
                        disableDialog.modal('setButtonModel', [{
                            buttonText: 'Disable',
                            handler: {
                                click: disableHandler
                            }
                        }, {
                            buttonText: 'Cancel',
                            handler: {
                                click: closeModal
                            }
                        }]);
                    }
                }
            }).draggable({
                containment: 'parent',
                handle: '.dialog-header'
            });
            
            // initialize the enable scope combo
            $('#enable-controller-service-scope').combo({
                options: [{
                        text: 'Service only',
                        value: config.serviceOnly,
                        description: 'Enable only this controller service'
                    }, {
                        text: 'Service and referencing components',
                        value: config.serviceAndReferencingComponents,
                        description: 'Enable this controller service and enable/start all referencing components'
                    }]
            });
            
            // initialize the enable service dialog
            $('#enable-controller-service-dialog').modal({
                headerText: 'Enable Controller Service',
                overlayBackground: false,
                buttons: [{
                    buttonText: 'Enable',
                    handler: {
                        click: enableHandler
                    }
                }, {
                    buttonText: 'Cancel',
                    handler: {
                        click: closeModal
                    }
                }],
                handler: {
                    close: function() {
                        var enableDialog = $(this);
                        
                        // reset visibility
                        $('#enable-controller-service-service-container').show();
                        $('#enable-controller-service-scope-container').show();
                        $('#enable-controller-service-progress-container').hide();
                        $('#enable-controller-service-progress li.referencing-component').show();
                        
                        // clear the dialog
                        $('#enable-controller-service-id').text('');
                        $('#enable-controller-service-name').text('');
                        
                        // reset progress
                        $('div.enable-referencing-components').removeClass('ajax-loading ajax-complete ajax-error');
                        
                        // referencing components
                        var referencingComponents = $('#enable-controller-service-referencing-components');
                        nf.Common.cleanUpTooltips(referencingComponents, 'div.referencing-component-state');
                        referencingComponents.css('border-width', '0').empty();
                        
                        // reset dialog
                        enableDialog.modal('setButtonModel', [{
                            buttonText: 'Disable',
                            handler: {
                                click: enableHandler
                            }
                        }, {
                            buttonText: 'Cancel',
                            handler: {
                                click: closeModal
                            }
                        }]);
                    }
                }
            }).draggable({
                containment: 'parent',
                handle: '.dialog-header'
            });
        },
        
        /**
         * Shows the configuration dialog for the specified controller service.
         * 
         * @argument {controllerService} controllerService      The controller service
         */
        showConfiguration: function (controllerService) {
            // reload the service in case the property descriptors have changed
            var reloadService = $.ajax({
                type: 'GET',
                url: controllerService.uri,
                dataType: 'json'
            });
            
            // get the controller service history
            var loadHistory = $.ajax({
                type: 'GET',
                url: '../nifi-api/controller/history/controller-services/' + encodeURIComponent(controllerService.id),
                dataType: 'json'
            });
            
            // once everything is loaded, show the dialog
            $.when(reloadService, loadHistory).done(function (serviceResponse, historyResponse) {
                // get the updated controller service
                controllerService = serviceResponse[0].controllerService;
                
                // get the controller service history
                var controllerServiceHistory = historyResponse[0].componentHistory;
                
                // record the controller service details
                $('#controller-service-configuration').data('controllerServiceDetails', controllerService);

                // populate the controller service settings
                $('#controller-service-id').text(controllerService['id']);
                $('#controller-service-type').text(nf.Common.substringAfterLast(controllerService['type'], '.'));
                $('#controller-service-name').val(controllerService['name']);
                $('#controller-service-enabled').removeClass('checkbox-checked checkbox-unchecked').addClass('checkbox-unchecked');
                $('#controller-service-comments').val(controllerService['comments']);

                // select the availability when appropriate
                if (nf.Canvas.isClustered()) {
                    if (controllerService['availability'] === 'node') {
                        $('#availability').text('Node');
                    } else {
                        $('#availability').text('Cluster Manager');
                    }
                }
                
                // get the reference container
                var referenceContainer = $('#controller-service-referencing-components');

                // load the controller referencing components list
                createReferencingComponents(referenceContainer, controllerService.referencingComponents);

                var buttons = [{
                        buttonText: 'Apply',
                        handler: {
                            click: function () {
                                // close all fields currently being edited
                                $('#controller-service-properties').propertytable('saveRow');

                                // marshal the settings and properties and update the controller service
                                var updatedControllerService = marshalDetails();

                                // ensure details are valid as far as we can tell
                                if (validateDetails(updatedControllerService)) {
                                    // update the selected component
                                    $.ajax({
                                        type: 'PUT',
                                        data: JSON.stringify(updatedControllerService),
                                        url: controllerService.uri,
                                        dataType: 'json',
                                        processData: false,
                                        contentType: 'application/json'
                                    }).done(function (response) {
                                        if (nf.Common.isDefinedAndNotNull(response.controllerService)) {
                                            nf.Client.setRevision(response.revision);

                                            // reload the controller service
                                            renderControllerService(response.controllerService);

                                            // close the details panel
                                            $('#controller-service-configuration').modal('hide');
                                        }
                                    }).fail(handleControllerServiceConfigurationError);
                                }
                            }
                        }
                    }, {
                        buttonText: 'Cancel',
                        handler: {
                            click: function () {
                                $('#controller-service-configuration').modal('hide');
                            }
                        }
                    }];

                // determine if we should show the advanced button
                if (nf.Common.isDefinedAndNotNull(controllerService.customUiUrl) && controllerService.customUiUrl !== '') {
                    buttons.push({
                        buttonText: 'Advanced',
                        handler: {
                            click: function () {
                                var openCustomUi = function () {
                                    // reset state and close the dialog manually to avoid hiding the faded background
                                    $('#controller-service-configuration').modal('hide');

                                    // show the custom ui
                                    nf.CustomProcessorUi.showCustomUi($('#controller-service-id').text(), controllerService.customUiUrl, true).done(function () {
                                        // once the custom ui is closed, reload the controller service
                                        reloadControllerService(controllerService);
                                    });
                                };

                                // close all fields currently being edited
                                $('#controller-service-properties').propertytable('saveRow');

                                // determine if changes have been made
                                if (isSaveRequired()) {
                                    // see if those changes should be saved
                                    nf.Dialog.showYesNoDialog({
                                        dialogContent: 'Save changes before opening the advanced configuration?',
                                        overlayBackground: false,
                                        noHandler: openCustomUi,
                                        yesHandler: function () {
                                            // marshal the settings and properties and update the controller service
                                            var updatedControllerService = marshalDetails();

                                            // ensure details are valid as far as we can tell
                                            if (validateDetails(updatedControllerService)) {
                                                // update the selected component
                                                $.ajax({
                                                    type: 'PUT',
                                                    data: JSON.stringify(updatedControllerService),
                                                    url: controllerService.uri,
                                                    dataType: 'json',
                                                    processData: false,
                                                    contentType: 'application/json'
                                                }).done(function (response) {
                                                    if (nf.Common.isDefinedAndNotNull(response.controllerService)) {
                                                        // update the revision
                                                        nf.Client.setRevision(response.revision);

                                                        // open the custom ui
                                                        openCustomUi();
                                                    }
                                                }).fail(handleControllerServiceConfigurationError);
                                            }
                                        }
                                    });
                                } else {
                                    // if there were no changes, simply open the custom ui
                                    openCustomUi();
                                }
                            }
                        }
                    });
                }

                // set the button model
                $('#controller-service-configuration').modal('setButtonModel', buttons);
                
                // load the property table
                $('#controller-service-properties').propertytable('loadProperties', controllerService.properties, controllerService.descriptors, controllerServiceHistory.propertyHistory);

                // show the details
                $('#controller-service-configuration').modal('show');

                // show the border if necessary
                updateReferencingComponentsBorder(referenceContainer);
            }).fail(nf.Common.handleAjaxError);
        }, 
        
        showDetails: function(controllerService) {
            
        },
        
        /**
         * Enables the specified controller service.
         * 
         * @param {object} controllerService
         */
        enable: function(controllerService) {
            if (nf.Common.isEmpty(controllerService.referencingComponents)) {
                setEnabled(controllerService, true);
            } else {
                showEnableControllerServiceDialog(controllerService);
            }
        },
        
        /**
         * Disables the specified controller service.
         * 
         * @param {object} controllerService
         */
        disable: function(controllerService) {
            if (nf.Common.isEmpty(controllerService.referencingComponents)) {
                setEnabled(controllerService, false);
            } else {
                showDisableControllerServiceDialog(controllerService);
            }
        },
        
        /**
         * Deletes the specified controller service.
         * 
         * @param {object} controllerService
         */
        remove: function(controllerService) {
            // prompt for removal?
                    
            var revision = nf.Client.getRevision();
            $.ajax({
                type: 'DELETE',
                url: controllerService.uri + '?' + $.param({
                    version: revision.version,
                    clientId: revision.clientId
                }),
                dataType: 'json'
            }).done(function (response) {
                nf.Client.setRevision(response.revision);

                // remove the service
                var controllerServiceGrid = $('#controller-services-table').data('gridInstance');
                var controllerServiceData = controllerServiceGrid.getData();
                controllerServiceData.deleteItem(controllerService.id);

                // reload the as necessary
                reloadControllerServiceReferences(controllerService);
            }).fail(nf.Common.handleAjaxError);
        }
    };
}());
