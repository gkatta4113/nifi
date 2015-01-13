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
        filterText: 'Filter',
        styles: {
            filterList: 'filter-list'
        },
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
     * Get the text out of the filter field. If the filter field doesn't
     * have any text it will contain the text 'filter list' so this method
     * accounts for that.
     */
    var getControllerServiceTypeFilterText = function () {
        var filterText = '';
        var filterField = $('#processor-type-filter');
        if (!filterField.hasClass(config.styles.filterList)) {
            filterText = filterField.val();
        }
        return filterText;
    };
    
    /**
     * Filters the processor type table.
     */
    var applyControllerServiceTypeFilter = function () {
        // get the dataview
        var controllerServiceTypesGrid = $('#controller-service-types-table').data('gridInstance');

        // ensure the grid has been initialized
        if (nf.Common.isDefinedAndNotNull(controllerServiceTypesGrid)) {
            var controllerServiceTypesData = controllerServiceTypesGrid.getData();

            // update the search criteria
            controllerServiceTypesData.setFilterArgs({
                searchString: getControllerServiceTypeFilterText(),
                property: $('#controller-service-type-filter-options').combo('getSelectedOption').value
            });
            controllerServiceTypesData.refresh();
        }
    };
    
    /**
     * Performs the filtering.
     * 
     * @param {object} item     The item subject to filtering
     * @param {object} args     Filter arguments
     * @returns {Boolean}       Whether or not to include the item
     */
    var filterControllerServiceTypes = function (item, args) {
        // determine if the item matches the filter
        var matchesFilter = matchesRegex(item, args);

        // determine if the row matches the selected tags
        var matchesTags = true;
        if (matchesFilter) {
            var tagFilters = $('#controller-service-tag-cloud').tagcloud('getSelectedTags');
            var hasSelectedTags = tagFilters.length > 0;
            if (hasSelectedTags) {
                matchesTags = matchesSelectedTags(tagFilters, item['tags']);
            }
        }

        // determine if this row should be visible
        var matches = matchesFilter && matchesTags;

        // if this row is currently selected and its being filtered
        if (matches === false && $('#selected-controller-service-type').text() === item['type']) {
            // clear the selected row
            $('#controller-service-type-description').text('');
            $('#controller-service-type-name').text('');
            $('#selected-controller-service-name').text('');
            $('#selected-controller-service-type').text('');

            // clear the active cell the it can be reselected when its included
            var controllerServiceTypesGrid = $('#controller-service-types-table').data('gridInstance');
            controllerServiceTypesGrid.resetActiveCell();
        }

        return matches;
    };
    
    /**
     * Determines if the item matches the filter.
     * 
     * @param {object} item     The item to filter
     * @param {object} args     The filter criteria
     * @returns {boolean}       Whether the item matches the filter
     */
    var matchesRegex = function (item, args) {
        if (args.searchString === '') {
            return true;
        }

        try {
            // perform the row filtering
            var filterExp = new RegExp(args.searchString, 'i');
        } catch (e) {
            // invalid regex
            return false;
        }

        // determine if the item matches the filter
        return item[args.property].search(filterExp) >= 0;
    };

    /**
     * Determines if the specified tags match all the tags selected by the user.
     * 
     * @argument {string[]} tagFilters      The tag filters
     * @argument {string} tags              The tags to test
     */
    var matchesSelectedTags = function (tagFilters, tags) {
        var selectedTags = [];
        $.each(tagFilters, function (_, filter) {
            selectedTags.push(filter);
        });

        // normalize the tags
        var normalizedTags = tags.toLowerCase();

        var matches = true;
        $.each(selectedTags, function (i, selectedTag) {
            if (normalizedTags.indexOf(selectedTag) === -1) {
                matches = false;
                return false;
            }
        });

        return matches;
    };
    
    /**
     * Sorts the specified data using the specified sort details.
     * 
     * @param {object} sortDetails
     * @param {object} data
     */
    var sort = function (sortDetails, data) {
        // defines a function for sorting
        var comparer = function (a, b) {
            var aString = nf.Common.isDefinedAndNotNull(a[sortDetails.columnId]) ? a[sortDetails.columnId] : '';
            var bString = nf.Common.isDefinedAndNotNull(b[sortDetails.columnId]) ? b[sortDetails.columnId] : '';
            return aString === bString ? 0 : aString > bString ? 1 : -1;
        };

        // perform the sort
        data.sort(comparer, sortDetails.sortAsc);
    };
    
    /**
     * Initializes the new controller service dialog.
     */
    var initNewControllerServiceDialog = function () {
        // specify the combo options
        $('#controller-service-type-filter-options').combo({
            options: [{
                    text: 'by type',
                    value: 'label'
                }, {
                    text: 'by tag',
                    value: 'tags'
                }],
            select: function (option) {
                applyControllerServiceTypeFilter();
            }
        });

        // initialize the processor type table
        var controllerServiceTypesColumns = [
            {id: 'type', name: 'Type', field: 'label', sortable: true, resizable: true},
            {id: 'tags', name: 'Tags', field: 'tags', sortable: true, resizable: true}
        ];
        var controllerServiceTypesOptions = {
            forceFitColumns: true,
            enableTextSelectionOnCells: true,
            enableCellNavigation: true,
            enableColumnReorder: false,
            autoEdit: false,
            multiSelect: false
        };

        // initialize the dataview
        var controllerServiceTypesData = new Slick.Data.DataView({
            inlineFilters: false
        });
        controllerServiceTypesData.setItems([]);
        controllerServiceTypesData.setFilterArgs({
            searchString: getControllerServiceTypeFilterText(),
            property: $('#controller-service-type-filter-options').combo('getSelectedOption').value
        });
        controllerServiceTypesData.setFilter(filterControllerServiceTypes);

        // initialize the sort
        sort({
            columnId: 'type',
            sortAsc: true
        }, controllerServiceTypesData);

        // initialize the grid
        var controllerServiceTypesGrid = new Slick.Grid('#controller-service-types-table', controllerServiceTypesData, controllerServiceTypesColumns, controllerServiceTypesOptions);
        controllerServiceTypesGrid.setSelectionModel(new Slick.RowSelectionModel());
        controllerServiceTypesGrid.registerPlugin(new Slick.AutoTooltips());
        controllerServiceTypesGrid.setSortColumn('type', true);
        controllerServiceTypesGrid.onSort.subscribe(function (e, args) {
            sort({
                columnId: args.sortCol.field,
                sortAsc: args.sortAsc
            }, controllerServiceTypesData);
        });
        controllerServiceTypesGrid.onSelectedRowsChanged.subscribe(function (e, args) {
            if ($.isArray(args.rows) && args.rows.length === 1) {
                var processorTypeIndex = args.rows[0];
                var processorType = controllerServiceTypesGrid.getDataItem(processorTypeIndex);

                // set the processor type description
                if (nf.Common.isBlank(processorType.description)) {
                    $('#controller-service-type-description').attr('title', '').html('<span class="unset">No description specified</span>');
                } else {
                    $('#controller-service-type-description').text(processorType.description).ellipsis();
                }

                // populate the dom
                $('#controller-service-type-name').text(processorType.label).ellipsis();
                $('#selected-controller-service-name').text(processorType.label);
                $('#selected-controller-service-type').text(processorType.type);
            }
        });

        // wire up the dataview to the grid
        controllerServiceTypesData.onRowCountChanged.subscribe(function (e, args) {
            controllerServiceTypesGrid.updateRowCount();
            controllerServiceTypesGrid.render();

            // update the total number of displayed processors
            $('#displayed-controller-service-types').text(args.current);
        });
        controllerServiceTypesData.onRowsChanged.subscribe(function (e, args) {
            controllerServiceTypesGrid.invalidateRows(args.rows);
            controllerServiceTypesGrid.render();
        });
        controllerServiceTypesData.syncGridSelection(controllerServiceTypesGrid, false);

        // hold onto an instance of the grid
        $('#controller-service-types-table').data('gridInstance', controllerServiceTypesGrid);
        
        // load the available controller services
        $.ajax({
            type: 'GET',
            url: config.urls.controllerServiceTypes,
            dataType: 'json'
        }).done(function(response) {
            var tags = [];
            console.log(response);

            // begin the update
            controllerServiceTypesData.beginUpdate();

            // go through each processor type
            $.each(response.controllerServiceTypes, function (i, documentedType) {
                var type = documentedType.type;

                // create the row for the processor type
                controllerServiceTypesData.addItem({
                    id: i,
                    label: nf.Common.substringAfterLast(type, '.'),
                    type: type,
                    description: nf.Common.escapeHtml(documentedType.description),
                    tags: documentedType.tags.join(', '),
                    baseType: documentedType.baseTypes
                });

                // count the frequency of each tag for this type
                $.each(documentedType.tags, function (i, tag) {
                    tags.push(tag.toLowerCase());
                });
            });

            // end the udpate
            controllerServiceTypesData.endUpdate();

            // set the total number of processors
            $('#total-controller-service-types, #displayed-controller-service-types').text(response.controllerServiceTypes.length);

            // create the tag cloud
            $('#controller-service-tag-cloud').tagcloud({
                tags: tags,
                select: applyControllerServiceTypeFilter,
                remove: applyControllerServiceTypeFilter
            });
        }).fail(nf.Common.handleAjaxError);
        
        // initialize the controller service dialog
        $('#new-controller-service-dialog').modal({
            headerText: 'Add Controller Service',
            overlayBackground: false,
            buttons: [{
                buttonText: 'Add',
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
            close: function() {
                // clear the selected row
                $('#controller-service-type-description').text('');
                $('#controller-service-type-name').text('');
                $('#selected-controller-service-name').text('');
                $('#selected-controller-service-type').text('');

                // unselect any current selection
                var processTypesGrid = $('#controller-service-types-table').data('gridInstance');
                processTypesGrid.setSelectedRows([]);
                processTypesGrid.resetActiveCell();

                // clear any filter strings
                $('#controller-service-type-filter').addClass(config.styles.filterList).val(config.filterText);

                // clear the tagcloud
                $('#controller-service-tag-cloud').tagcloud('clearSelectedTags');
            }
        });
    };
    
    /**
     * Initializes the controller services tab.
     */
    var initControllerServices = function () {
        // initialize the new controller service dialog
        initNewControllerServiceDialog();
        
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
     * Initializes the new reporting task dialog.
     */
    var initNewReportingTaskDialog = function () {
        $.ajax({
            type: 'GET',
            url: config.urls.reportingTaskTypes,
            dataType: 'json'
        }).done(function(response) {
            console.log(response);
        });
        
    };
    
    /**
     * Initializes the reporting tasks tab.
     */
    var initReportingTasks = function () {
        // initialize the new reporting task dialog
        initNewReportingTaskDialog();
        
        var moreReportingTaskDetails = function (row, cell, value, columnDef, dataContext) {
            return '<img src="images/iconDetails.png" title="View Details" class="pointer" style="margin-top: 5px; float: left;" onclick="javascript:nf.Settings.showReportingTaskDetails(\'' + row + '\');"/>';
        };
        
        // define the column model for the reporting tasks table
        var reportingTasksColumnModel = [
            {id: 'moreDetails', field: 'moreDetails', name: '&nbsp;', resizable: false, formatter: moreReportingTaskDetails, sortable: true, width: 50, maxWidth: 50},
            {id: 'id', field: 'id', name: 'Identifier', sortable: true, resizable: true},
            {id: 'type', field: 'type', name: 'Type', sortable: true, resizable: true}
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
                    $('#new-controller-service-dialog').modal('show');
                } else if (selectedTab === 'Reporting Tasks') {
                    
                }
            });

            // initialize each tab
            initGeneral();
            initControllerServices();
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
         * Shows the details of the controller service at the specified row.
         * 
         * @param {type} row
         */
        showControllerServiceDetails: function (row) {
            
        },
        
        /**
         * Shows the details of the reporting task at the specified row.
         * 
         * @param {type} row
         */
        showReportingTaskDetails: function (row) {
            
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