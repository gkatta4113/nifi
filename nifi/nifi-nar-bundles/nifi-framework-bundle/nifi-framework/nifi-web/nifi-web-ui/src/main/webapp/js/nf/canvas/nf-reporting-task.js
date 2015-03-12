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

/* global nf */

nf.ReportingTask = (function () {

    /**
     * Handle any expected reporting task configuration errors.
     * 
     * @argument {object} xhr       The XmlHttpRequest
     * @argument {string} status    The status of the request
     * @argument {string} error     The error
     */
    var handleReportingTaskConfigurationError = function (xhr, status, error) {
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
     * Determines whether the user has made any changes to the reporting task configuration
     * that needs to be saved.
     */
    var isSaveRequired = function () {
        var details = $('#reporting-task-configuration').data('reportingTaskDetails');

        // determine if any reporting task settings have changed

        if ($('#reporting-task-name').val() !== details.name) {
            return true;
        }
        if ($('#reporting-task-enabled').hasClass('checkbox-checked') && details['state'] === 'DISABLED') {
            return true;
        } else if ($('#reporting-task-enabled').hasClass('checkbox-unchecked') && (details['state'] === 'RUNNING' || details['state'] === 'STOPPED')) {
            return true;
        }
        
        // defer to the properties
        return $('#reporting-task-properties').propertytable('isSaveRequired');
    };

    /**
     * Marshals the data that will be used to update the reporting task's configuration.
     */
    var marshalDetails = function () {
        // properties
        var properties = $('#reporting-task-properties').propertytable('marshalProperties');

        // create the reporting task dto
        var reportingTaskDto = {};
        reportingTaskDto['id'] = $('#reporting-task-id').text();
        reportingTaskDto['name'] = $('#reporting-task-name').val();
        
        // mark the processor disabled if appropriate
        if ($('#reporting-task-enabled').hasClass('checkbox-unchecked')) {
            reportingTaskDto['state'] = 'DISABLED';
        } else if ($('#reporting-task-enabled').hasClass('checkbox-checked')) {
            reportingTaskDto['state'] = 'STOPPED';
        }
        
        // set the properties
        if ($.isEmptyObject(properties) === false) {
            reportingTaskDto['properties'] = properties;
        }
        
        // create the reporting task entity
        var reportingTaskEntity = {};
        reportingTaskEntity['revision'] = nf.Client.getRevision();
        reportingTaskEntity['reportingTask'] = reportingTaskDto;

        // return the marshaled details
        return reportingTaskEntity;
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
     * Reloads the specified reporting task.
     * 
     * @param {object} reportingTask
     */
    var reloadReportingTask = function (reportingTask) {
        return $.ajax({
            type: 'GET',
            url: reportingTask.uri,
            dataType: 'json'
        }).done(function (response) {
            renderReportingTask(response.reportingTask);
        }).fail(nf.Common.handleAjaxError);
    };
    
    /**
     * Renders the specified reporting task.
     * 
     * @param {object} reportingTask
     */
    var renderReportingTask = function (reportingTask) {
        // get the table and update the row accordingly
        var reportingTaskGrid = $('#reporting-tasks-table').data('gridInstance');
        var reportingTaskData = reportingTaskGrid.getData();
        reportingTaskData.updateItem(reportingTask.id, reportingTask);
    };
    
    /**
     * 
     * @param {object} reportingTask
     * @param {boolean} running
     */
    var setRunning = function (reportingTask, running) {
        var revision = nf.Client.getRevision();
        return $.ajax({
            type: 'PUT',
            url: reportingTask.uri,
            data: {
                clientId: revision.clientId,
                version: revision.version,
                state: running === true ? 'RUNNING' : 'STOPPED'
            },
            dataType: 'json'
        }).done(function (response) {
            // update the revision
            nf.Client.setRevision(response.revision);
            
            // update the task
            renderReportingTask(response.reportingTask);
        }).fail(nf.Common.handleAjaxError);
    };
    
    return {
        /**
         * Initializes the reporting task configuration dialog.
         */
        init: function () {
            // initialize the configuration dialog tabs
            $('#reporting-task-configuration-tabs').tabbs({
                tabStyle: 'tab',
                selectedTabStyle: 'selected-tab',
                tabs: [{
                        name: 'Settings',
                        tabContentId: 'reporting-task-standard-settings-tab-content'
                    }, {
                        name: 'Properties',
                        tabContentId: 'reporting-task-properties-tab-content'
                    }],
                select: function () {
                    // update the property table size in case this is the first time its rendered
                    if ($(this).text() === 'Properties') {
                        $('#reporting-task-properties').propertytable('resetTableSize');
                    }

                    // close all fields currently being edited
                    $('#reporting-task-properties').propertytable('saveRow');
                }
            });
            
            // we clustered we need to show the controls for editing the availability
            if (nf.Canvas.isClustered()) {
                $('#availability-setting-container').show();
            }

            // initialize the reporting task configuration dialog
            $('#reporting-task-configuration').modal({
                headerText: 'Configure Reporting Task',
                overlayBackground: false,
                handler: {
                    close: function () {
                        // cancel any active edits
                        $('#reporting-task-properties').propertytable('cancelEdit');

                        // clear the tables
                        $('#reporting-task-properties').propertytable('clear');
                        
                        // removed the cached reporting task details
                        $('#reporting-task-configuration').removeData('reportingTaskDetails');
                    }
                }
            }).draggable({
                containment: 'parent',
                handle: '.dialog-header'
            });

            // initialize the property table
            $('#reporting-task-properties').propertytable({
                readOnly: false,
                newPropertyDialogContainer: 'body'
            });
        },
        
        /**
         * Shows the configuration dialog for the specified reporting task.
         * 
         * @argument {reportingTask} reportingTask      The reporting task
         */
        showConfiguration: function (reportingTask) {
            // reload the task in case the property descriptors have changed
            var reloadTask = $.ajax({
                type: 'GET',
                url: reportingTask.uri,
                dataType: 'json'
            });
            
            // get the reporting task history
            var loadHistory = $.ajax({
                type: 'GET',
                url: '../nifi-api/controller/history/reporting-tasks/' + encodeURIComponent(reportingTask.id),
                dataType: 'json'
            });
            
            // once everything is loaded, show the dialog
            $.when(reloadTask, loadHistory).done(function (taskResponse, historyResponse) {
                // get the updated reporting task
                reportingTask = taskResponse[0].reportingTask;
                
                // get the reporting task history
                var reportingTaskHistory = historyResponse[0].componentHistory;
                
                // record the reporting task details
                $('#reporting-task-configuration').data('reportingTaskDetails', reportingTask);

                // determine if the enabled checkbox is checked or not
                var reportingTaskEnableStyle = 'checkbox-checked';
                if (reportingTask['state'] === 'DISABLED') {
                    reportingTaskEnableStyle = 'checkbox-unchecked';
                }
                
                // populate the reporting task settings
                $('#reporting-task-id').text(reportingTask['id']);
                $('#reporting-task-type').text(nf.Common.substringAfterLast(reportingTask['type'], '.'));
                $('#reporting-task-name').val(reportingTask['name']);
                $('#reporting-task-enabled').removeClass('checkbox-unchecked checkbox-checked').addClass(reportingTaskEnableStyle);

                // select the availability when appropriate
                if (nf.Canvas.isClustered()) {
                    if (reportingTask['availability'] === 'node') {
                        $('#availability').text('Node');
                    } else {
                        $('#availability').text('Cluster Manager');
                    }
                }
                
                var buttons = [{
                        buttonText: 'Apply',
                        handler: {
                            click: function () {
                                // close all fields currently being edited
                                $('#reporting-task-properties').propertytable('saveRow');

                                // marshal the settings and properties and update the reporting task
                                var updatedReportingTask = marshalDetails();

                                // ensure details are valid as far as we can tell
                                if (validateDetails(updatedReportingTask)) {
                                    // update the selected component
                                    $.ajax({
                                        type: 'PUT',
                                        data: JSON.stringify(updatedReportingTask),
                                        url: reportingTask.uri,
                                        dataType: 'json',
                                        processData: false,
                                        contentType: 'application/json'
                                    }).done(function (response) {
                                        if (nf.Common.isDefinedAndNotNull(response.reportingTask)) {
                                            // update the revision
                                            nf.Client.setRevision(response.revision);

                                            // reload the reporting task
                                            renderReportingTask(response.reportingTask);

                                            // close the details panel
                                            $('#reporting-task-configuration').modal('hide');
                                        }
                                    }).fail(handleReportingTaskConfigurationError);
                                }
                            }
                        }
                    }, {
                        buttonText: 'Cancel',
                        handler: {
                            click: function () {
                                $('#reporting-task-configuration').modal('hide');
                            }
                        }
                    }];

                // determine if we should show the advanced button
                if (nf.Common.isDefinedAndNotNull(reportingTask.customUiUrl) && reportingTask.customUiUrl !== '') {
                    buttons.push({
                        buttonText: 'Advanced',
                        handler: {
                            click: function () {
                                var openCustomUi = function () {
                                    // reset state and close the dialog manually to avoid hiding the faded background
                                    $('#reporting-task-configuration').modal('hide');

                                    // show the custom ui
                                    nf.CustomProcessorUi.showCustomUi($('#reporting-task-id').text(), reportingTask.customUiUrl, true).done(function () {
                                        // once the custom ui is closed, reload the reporting task
                                        reloadReportingTask(reportingTask);
                                    });
                                };

                                // close all fields currently being edited
                                $('#reporting-task-properties').propertytable('saveRow');

                                // determine if changes have been made
                                if (isSaveRequired()) {
                                    // see if those changes should be saved
                                    nf.Dialog.showYesNoDialog({
                                        dialogContent: 'Save changes before opening the advanced configuration?',
                                        overlayBackground: false,
                                        noHandler: openCustomUi,
                                        yesHandler: function () {
                                            // marshal the settings and properties and update the reporting task
                                            var updatedReportingTask = marshalDetails();

                                            // ensure details are valid as far as we can tell
                                            if (validateDetails(updatedReportingTask)) {
                                                // update the selected component
                                                $.ajax({
                                                    type: 'PUT',
                                                    data: JSON.stringify(updatedReportingTask),
                                                    url: reportingTask.uri,
                                                    dataType: 'json',
                                                    processData: false,
                                                    contentType: 'application/json'
                                                }).done(function (response) {
                                                    if (nf.Common.isDefinedAndNotNull(response.reportingTask)) {
                                                        // update the revision
                                                        nf.Client.setRevision(response.revision);

                                                        // open the custom ui
                                                        openCustomUi();
                                                    }
                                                }).fail(handleReportingTaskConfigurationError);
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
                $('#reporting-task-configuration').modal('setButtonModel', buttons);
                
                // load the property table
                $('#reporting-task-properties').propertytable('loadProperties', reportingTask.properties, reportingTask.descriptors, reportingTaskHistory.propertyHistory);

                // show the details
                $('#reporting-task-configuration').modal('show');
            }).fail(nf.Common.handleAjaxError);
        }, 
        
        showDetails: function(reportingTask) {
            
        },
        
        /**
         * Starts the specified reporting task.
         * 
         * @param {object} reportingTask
         */
        start: function(reportingTask) {
            setRunning(reportingTask, true);
        },
        
        /**
         * Stops the specified reporting task.
         * 
         * @param {object} reportingTask
         */
        stop: function(reportingTask) {
            setRunning(reportingTask, false);
        },
        
        /**
         * Deletes the specified reporting task.
         * 
         * @param {object} reportingTask
         */
        remove: function(reportingTask) {
            // prompt for removal?
                    
            var revision = nf.Client.getRevision();
            $.ajax({
                type: 'DELETE',
                url: reportingTask.uri + '?' + $.param({
                    version: revision.version,
                    clientId: revision.clientId
                }),
                dataType: 'json'
            }).done(function (response) {
                // update the revision
                nf.Client.setRevision(response.revision);

                // remove the task
                var reportingTaskGrid = $('#reporting-tasks-table').data('gridInstance');
                var reportingTaskData = reportingTaskGrid.getData();
                reportingTaskData.deleteItem(reportingTask.id);
            }).fail(nf.Common.handleAjaxError);
        }
    };
}());
