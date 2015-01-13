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
nf.Settings = (function () {

    var config = {
        urls: {
            controllerConfig: '../nifi-api/controller/config',
            controllerArchive: '../nifi-api/controller/archive',
            controllerServiceTypes: '../nifi-api/controller/controller-service-types',
            reportingTaskTypes: '../nifi-api/controller/reporting-task-types'
        }
    };

    /**
     * Initializes the general tab.
     */
    var initGeneral = function () {
        // register the click listener for the archive link
        $('#archive-flow-link').click(function () {
            var revision = nf.Client.getRevision();

            $.ajax({
                type: 'POST',
                url: config.urls.controllerArchive,
                data: {
                    version: revision.version,
                    clientId: revision.clientId
                },
                dataType: 'json'
            }).done(function (response) {
                // update the revision
                nf.Client.setRevision(response.revision);

                // show the result dialog
                nf.Dialog.showOkDialog({
                    dialogContent: 'A new flow archive was successfully created.',
                    overlayBackground: false
                });
            }).fail(nf.Common.handleAjaxError);
        });

        // register the click listener for the save button
        $('#settings-save').click(function () {
            var revision = nf.Client.getRevision();

            // marshal the configuration details
            var configuration = marshalConfiguration();
            configuration['version'] = revision.version;
            configuration['clientId'] = revision.clientId;

            // save the new configuration details
            $.ajax({
                type: 'PUT',
                url: config.urls.controllerConfig,
                data: configuration,
                dataType: 'json'
            }).done(function (response) {
                // update the revision
                nf.Client.setRevision(response.revision);

                // update the displayed name
                document.title = response.config.name;

                // set the data flow title and close the shell
                $('#data-flow-title-container').children('span.link:first-child').text(response.config.name);

                // close the settings dialog
                nf.Dialog.showOkDialog({
                    dialogContent: 'Settings successfully applied.',
                    overlayBackground: false
                });
            }).fail(nf.Common.handleAjaxError);
        });
    };

    /**
     * Marshals the details to include in the configuration request.
     */
    var marshalConfiguration = function () {
        // create the configuration
        var configuration = {};
        configuration['name'] = $('#data-flow-title-field').val();
        configuration['comments'] = $('#data-flow-comments-field').val();
        configuration['maxTimerDrivenThreadCount'] = $('#maximum-timer-driven-thread-count-field').val();
        configuration['maxEventDrivenThreadCount'] = $('#maximum-event-driven-thread-count-field').val();
        return configuration;
    };
    
    
    
    /**
     * Initializes the controller services tab.
     */
    var initControllerServies = function () {
        $.ajax({
            type: 'GET',
            url: config.urls.controllerServiceTypes,
            dataType: 'json'
        }).done(function(response) {
            console.log(response);
        });
        
        var moreControllerServiceDetails = function (row, cell, value, columnDef, dataContext) {
            return '<img src="images/iconDetails.png" title="View Details" class="pointer" style="margin-top: 5px; float: left;" onclick="javascript:nf.Settings.showControllerServiceDetails(\'' + row + '\');"/>';
        };
        
        // define the column model for the controller services table
        var controllerServicesColumnModel = [
            {id: 'moreDetails', field: 'moreDetails', name: '&nbsp;', resizable: false, formatter: moreControllerServiceDetails, sortable: true, width: 50, maxWidth: 50},
            {id: 'id', field: 'id', name: 'Identifier', sortable: true, resizable: true},
            {id: 'type', field: 'type', name: 'Type', sortable: true, resizable: true}
        ];
    };
    
    /**
     * Initializes the reporting tasks tab.
     */
    var initReportingTasks = function () {
        $.ajax({
            type: 'GET',
            url: config.urls.reportingTaskTypes,
            dataType: 'json'
        }).done(function(response) {
            console.log(response);
        });
        
        var moreReportingTaskDetails = function (row, cell, value, columnDef, dataContext) {
            return '<img src="images/iconDetails.png" title="View Details" class="pointer" style="margin-top: 5px; float: left;" onclick="javascript:nf.Settings.showControllerServiceDetails(\'' + row + '\');"/>';
        };
        
        // define the column model for the reporting tasks table
        var reportingTasksColumnModel = [
            {id: 'moreDetails', field: 'moreDetails', name: '&nbsp;', resizable: false, formatter: moreReportingTaskDetails, sortable: true, width: 50, maxWidth: 50},
            {id: 'id', field: 'id', name: 'Identifier', sortable: true, resizable: true},
            {id: 'type', field: 'type', name: 'Type', sortable: true, resizable: true},
            
        ];
    };

    return {
        /**
         * Initializes the status page.
         */
        init: function () {
            // initialize the settings tabs
            $('#settings-tabs').tabbs({
                tabStyle: 'settings-tab',
                selectedTabStyle: 'settings-selected-tab',
                tabs: [{
                    name: 'General',
                    tabContentId: 'general-settings-tab-content'
                }, {
                    name: 'Controller Services',
                    tabContentId: 'controller-services-tab-content'
                }, {
                    name: 'Reporting Tasks',
                    tabContentId: 'reporting-tasks-tab-content'
                }],
                select: function () {
                    var tab = $(this).text();
                    if (tab === 'General') {
                        $('#new-service-or-task').hide();
                    } else {
                        $('#new-service-or-task').show();
                        
                        // update the tooltip on the button
                        $('#new-service-or-task').attr('title', function() {
                            if (tab === 'Controller Services') {
                                return 'Create a new controller service';
                            } else if (tab === 'Reporting Tasks') {
                                return 'Create a new reporting task';
                            }
                        });
                    }
                }
            });
            
            // create a new controller service or reporting task
            $('#new-service-or-task').on('click', function() {
                var selectedTab = $('li.settings-selected-tab').text();
                if (selectedTab === 'Controller Services') {
                    
                } else if (selectedTab === 'Reporting Tasks') {
                    
                }
            });

            // initialize each tab
            initGeneral();
            initControllerServies();
            initReportingTasks();
        },
        
        /**
         * Update the size of the grid based on its container's current size.
         */
        resetTableSize: function () {
            var controllerServicesGrid = $('#controller-services-table').data('gridInstance');
            if (nf.Common.isDefinedAndNotNull(controllerServicesGrid)) {
                controllerServicesGrid.resizeCanvas();
            }

            var reportingTasksGrid = $('#reporting-tasks-table').data('gridInstance');
            if (nf.Common.isDefinedAndNotNull(reportingTasksGrid)) {
                reportingTasksGrid.resizeCanvas();
            }
        },
        
        /**
         * Shows the settings dialog.
         */
        showSettings: function () {
            $.ajax({
                type: 'GET',
                url: config.urls.controllerConfig,
                dataType: 'json'
            }).done(function (response) {
                // ensure the config is present
                if (nf.Common.isDefinedAndNotNull(response.config)) {
                    // set the header
                    $('#settings-header-text').text(response.config.name + ' Settings');

                    // populate the controller settings
                    $('#data-flow-title-field').val(response.config.name);
                    $('#data-flow-comments-field').val(response.config.comments);
                    $('#maximum-timer-driven-thread-count-field').val(response.config.maxTimerDrivenThreadCount);
                    $('#maximum-event-driven-thread-count-field').val(response.config.maxEventDrivenThreadCount);
                }

                // show the settings dialog
                nf.Shell.showContent('#settings').done(function () {
                    // reset button state
                    $('#settings-save').mouseout();
                });
            }).fail(nf.Common.handleAjaxError);
        }
    };
}());