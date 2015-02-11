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
nf.ControllerService = (function () {

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
        
        if ($('#controller-service-enabled').hasClass('checkbox-checked') && details['enabled'] === false) {
            return true;
        } else if ($('#controller-service-enabled').hasClass('checkbox-unchecked') && details['enabled'] === true) {
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
        
        // mark the controller service disabled if appropriate
        if ($('#controller-service-enabled').hasClass('checkbox-unchecked')) {
            controllerServiceDto['enabled'] = false;
        } else if ($('#controller-service-enabled').hasClass('checkbox-checked')) {
            controllerServiceDto['enabled'] = true;
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
                if (nf.Canvas.getGroupId() === reference.groupId) {
                    var processor = nf.Processor.get(reference.id);
                    nf.Processor.reload(processor.component);
                }
            } else {
                reloadOther = true;
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
     * Adds the specified reference for this controller service.
     * 
     * @argument {jQuery} referenceContainer 
     * @param {array} referencingComponents
     */
    var createReferencingComponents = function (referenceContainer, referencingComponents) {
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
                
                var activeThreadCount = $('<span class="referencing-component-active-thread-count"></span>');
                if (nf.Common.isDefinedAndNotNull(referencingComponent.activeThreadCount) && referencingComponent.activeThreadCount > 0) {
                    activeThreadCount.text('(' + referencingComponent.activeThreadCount + ')');
                }
                var processorState = $('<div class="referencing-component-state"></div>').addClass(referencingComponent.state.toLowerCase());
                var processorType = $('<span class="referencing-component-type"></span>').text(nf.Common.substringAfterLast(referencingComponent.type, '.'));
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
                        if (!nf.Common.isEmpty(referencingService.referencingComponents)) {
                            createReferencingComponents(referencingServiceReferencesContainer, referencingService.referencingComponents);
                        } else {
                            referencingServiceReferencesContainer.append('<div class="unset">This service has no components referencing it.</div>');
                        }
                    } else {
                        referencingServiceReferencesContainer.empty();
                    }
                    
                    // toggle visibility
                    toggle(serviceTwist, referencingServiceReferencesContainer);
                    
                    // update borders as necessary
                    updateReferencingComponentsBorder(referenceContainer);
                });
                
                var serviceState = $('<div class="referencing-component-state"></div>').addClass(referencingComponent.enabled === true ? 'enabled' : 'disabled');
                var serviceType = $('<span class="referencing-component-type"></span>').text(nf.Common.substringAfterLast(referencingComponent.type, '.'));
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
    
    // sets whether the specified controller service is enabled
    var setEnabled = function (controllerService, enabled) {
        var revision = nf.Client.getRevision();
        return $.ajax({
            type: 'PUT',
            url: controllerService.uri,
            data: {
                clientId: revision.clientId,
                version: revision.version,
                enabled: enabled
            },
            dataType: 'json'
        }).done(function (response) {
            // update the revision
            nf.Client.setRevision(response.revision);
            
            // update the service
            renderControllerService(response.controllerService);
        }).fail(nf.Common.handleAjaxError);
    };
    
    /**
     * Shows the dialog for disabling a controller service.
     * 
     * @argument {object} controllerService The controller service to disable
     */
    var showDisableControllerServiceDialog = function (controllerService) {
        // populate the disable controller service dialog
        $('#disable-controller-service-name').text(controllerService.name);
        
        var referencingComponentsContainer = $('#disable-controller-service-referencing-components');
        
        // load the controller referencing components list
        if (!nf.Common.isEmpty(controllerService.referencingComponents)) {
            createReferencingComponents(referencingComponentsContainer, controllerService.referencingComponents);
        } else {
            referencingComponentsContainer.append('<div class="unset">This service has no components referencing it.</div>');
        }
        
        // show the dialog
        $('#disable-controller-service-dialog').modal('show');
        
        // update the border if necessary
        updateReferencingComponentsBorder(referencingComponentsContainer);
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
                        $('#controller-service-referencing-components').css('border-width', '0').empty();

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
                        click: function () {

                        }
                    }
                }, {
                    buttonText: 'Cancel',
                    handler: {
                        click: function () {
                            $(this).modal('hide');
                        }
                    }
                }],
                handler: {
                    close: function() {
                        $('#disable-controller-service-name').text('');
                        $('#disable-controller-service-referencing-components').css('border-width', '0').empty();
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

                // determine if the enabled checkbox is checked or not
                var controllerServiceEnableStyle = 'checkbox-checked';
                if (controllerService['enabled'] === false) {
                    controllerServiceEnableStyle = 'checkbox-unchecked';
                }

                // populate the controller service settings
                $('#controller-service-id').text(controllerService['id']);
                $('#controller-service-type').text(nf.Common.substringAfterLast(controllerService['type'], '.'));
                $('#controller-service-name').val(controllerService['name']);
                $('#controller-service-enabled').removeClass('checkbox-unchecked checkbox-checked').addClass(controllerServiceEnableStyle);
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
                if (!nf.Common.isEmpty(controllerService.referencingComponents)) {
                    createReferencingComponents(referenceContainer, controllerService.referencingComponents);
                } else {
                    referenceContainer.append('<div class="unset">This service has no components referencing it.</div>');
                }

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
                                            // update the revision
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
            setEnabled(controllerService, true);
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
                // update the revision
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
