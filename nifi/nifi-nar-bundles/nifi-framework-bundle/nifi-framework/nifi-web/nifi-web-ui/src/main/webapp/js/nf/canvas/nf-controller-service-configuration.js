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
     * Determines whether the user has made any changes to the processor configuration
     * that needs to be saved.
     */
    var isSaveRequired = function () {
        var details = $('#controller-service-configuration').data('controllerServiceDetails');

        // determine if any controller service settings have changed

        if ($('#controller-service-comments').val() !== details.comments) {
            return true;
        }

        // defer to the property and relationship grids
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
                    // update the processor property table size in case this is the first time its rendered
                    if ($(this).text() === 'Properties') {
                        $('#controller-service-properties').propertytable('resetTableSize');
                    }

                    // close all fields currently being edited
                    $('#controller-service-properties').propertytable('saveRow');

                    // show the border around the processor relationships if necessary
//                    var processorRelationships = $('#auto-terminate-relationship-names');
//                    if (processorRelationships.is(':visible') && processorRelationships.get(0).scrollHeight > processorRelationships.innerHeight()) {
//                        processorRelationships.css('border-width', '1px');
//                    }
                }
            });

            // initialize the processor configuration dialog
            $('#controller-service-configuration').modal({
                headerText: 'Configure Controller Service',
                overlayBackground: false,
                handler: {
                    close: function () {
//                        // empty the relationship list
//                        $('#auto-terminate-relationship-names').css('border-width', '0').empty();

                        // close the new property dialog if necessary
                        $('#processor-property-dialog').hide();

                        // cancel any active edits
                        $('#controller-service-properties').propertytable('cancelEdit');

                        // clear the tables
                        $('#controller-service-properties').propertytable('clear');

                        // removed the cached controller service details
//                        $('#controller-service-configuration').removeData('processorDetails');
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
            // record the processor details
            $('#controller-service-configuration').data('controllerServiceDetails', controllerService);

            // determine if the enabled checkbox is checked or not
            var controllerServiceEnableStyle = 'checkbox-checked';
            if (controllerService['enabled'] === false) {
                controllerServiceEnableStyle = 'checkbox-unchecked';
            }

            // populate the processor settings
            $('#controller-service-id').text(controllerService['id']);
            $('#controller-service-type').text(nf.Common.substringAfterLast(controllerService['type'], '.'));
            $('#controller-service-name').val(controllerService['name']);
            $('#controller-service-enabled').removeClass('checkbox-unchecked checkbox-checked').addClass(controllerServiceEnableStyle);
            $('#controller-service-comments').val(controllerService['comments']);

            // load the property table
            $('#controller-service-properties').propertytable('loadProperties', controllerService.properties, controllerService.descriptors, {});

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
//                                    if (nf.Common.isDefinedAndNotNull(response.processor)) {
//                                        // update the revision
//                                        nf.Client.setRevision(response.revision);
//
//                                        // set the new processor state based on the response
//                                        nf.Processor.set(response.processor);
//
//                                        // reload the processor's outgoing connections
//                                        reloadProcessorConnections(processor);
//
//                                        // close the details panel
//                                        $('#processor-configuration').modal('hide');
//                                    }
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
//                                    // once the custom ui is closed, reload the processor
//                                    nf.Processor.reload(processor);
//
//                                    // and reload the processor's outgoing connections
//                                    reloadProcessorConnections(processor);
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

//                // show the border if necessary
//                var processorRelationships = $('#auto-terminate-relationship-names');
//                if (processorRelationships.is(':visible') && processorRelationships.get(0).scrollHeight > processorRelationships.innerHeight()) {
//                    processorRelationships.css('border-width', '1px');
//                }
//            }).fail(nf.Common.handleAjaxError);
        }
    };
}());
