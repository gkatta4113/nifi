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
nf.ControllerServiceConfiguration = (function () {

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
        
        if (nf.Canvas.isClustered()) {
            if ($('#availability-combo').combo('getSelectedOption').value !== (details['availability'] + '')) {
                return true;
            }
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

        // add the availability if appropriate
        if (nf.Canvas.isClustered()) {
            controllerServiceDto['availability'] = $('#availability-combo').combo('getSelectedOption').value;
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
     * Reloads the specified controller service.
     * 
     * @param {object} controllerService
     */
    var renderControllerService = function (controllerService) {
        // get the table and update the row accordingly
        var controllerServiceGrid = $('#controller-services-table').data('gridInstance');
        var controllerServiceData = controllerServiceGrid.getData();
        controllerServiceData.updateItem(controllerService.id, controllerService);

        // reload the controller service references
        reloadControllerServiceReferences(controllerService);
    };
    
    /**
     * Adds a border to the controller service references if necessary.
     */
    var showReferencesBorder = function () {
        var controllerServiceReferences = $('#controller-service-references');
        if (controllerServiceReferences.is(':visible') && controllerServiceReferences.get(0).scrollHeight > controllerServiceReferences.innerHeight()) {
            controllerServiceReferences.css('border-width', '1px');
        }
    };
    
    /**
     * Adds the specified reference for this controller service.
     * 
     * @param {object} reference
     */
    var createReference = function (reference) {
        
    };
    
    /**
     * Reloads components that reference this controller service.
     * 
     * @param {object} controllerService
     */
    var reloadControllerServiceReferences = function (controllerService) {
        
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
                    showReferencesBorder();
                }
            });
            
            // we clustered we need to show the controls for editing the availability
            if (nf.Canvas.isClustered()) {
                $('#availability-combo').combo({
                    options: [{
                        text: 'Cluster Manager',
                        value: 'CLUSTER_MANAGER_ONLY',
                        description: 'This controller service will be available on the cluster manager only.'
                    }, {
                        text: 'Node',
                        value: 'NODE_ONLY',
                        description: 'This controller service will be available on the nodes only.'
                    }, {
                        text: 'Both',
                        value: 'BOTH',
                        description: 'This controller service will be available on the cluster manager and the nodes.'
                    }]
                });
                $('#availability-setting-container').show();
            }

            // initialize the conroller service configuration dialog
            $('#controller-service-configuration').modal({
                headerText: 'Configure Controller Service',
                overlayBackground: false,
                handler: {
                    close: function () {
                        // empty the references list
                        $('#controller-service-references').css('border-width', '0').empty();

                        // cancel any active edits
                        $('#controller-service-properties').propertytable('cancelEdit');

                        // clear the tables
                        $('#controller-service-properties').propertytable('clear');

                        // removed the cached controller service details
                        $('#controller-service-configuration').removeData('controllerServiceDetails');
//                        $('#controller-service-configuration').removeData('processorHistory');
                    }
                }
            });

            // initialize the property table
            $('#controller-service-properties').propertytable({
                readOnly: false,
                newPropertyDialogContainer: 'body'
            });
        },
        
        /**
         * Shows the configuration dialog for the specified controller service.
         * 
         * @argument {controllerService} controllerService      The controller service
         */
        showConfiguration: function (controllerService) {
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
                $('#availability-combo').combo('setSelectedOption', {
                    value: controllerService['availability']
                });
            }

            // load the property table
            $('#controller-service-properties').propertytable('loadProperties', controllerService.properties, controllerService.descriptors, {});

            // load the controller references list
            if (!nf.Common.isEmpty(controllerService.references)) {
                $.each(controllerService.references, function (_, reference) {
                    createReference(reference);
                });
            } else {
                $('#controller-service-references').append('<div class="unset">This service has no components referencing it.</div>');
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

            // get the processor history
//            $.ajax({
//                type: 'GET',
//                url: '../nifi-api/controller/history/processors/' + encodeURIComponent(processor.id),
//                dataType: 'json'
//            }).done(function (response) {
//                var processorHistory = response.processorHistory;
//
//                // record the processor history
//                $('#processor-configuration').data('processorHistory', processorHistory);

                // show the details
                $('#controller-service-configuration').modal('show');

                // show the border if necessary
                showReferencesBorder();
//            }).fail(nf.Common.handleAjaxError);
        }
    };
}());
