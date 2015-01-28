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
            controllerServices: '../nifi-api/controller/controller-services',
            reportingTaskTypes: '../nifi-api/controller/reporting-task-types',
            reportingTasks: '../nifi-api/controller/reporting-tasks'
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
        var filterField = $('#controller-service-type-filter');
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
            
            // need to invalidate the entire table since parent elements may need to be 
            // rerendered due to changes in their children
            controllerServiceTypesData.refresh();
            controllerServiceTypesGrid.invalidate();
        }
    };
    
    /**
     * Determines if all of the ancestors of the specified item are expanded.
     * 
     * @param {type} item
     * @returns {Boolean}
     */
    var areAncestorsExpanded = function (item) {
        var documentedType = item;
        while (documentedType.parent !== null) {
            if (documentedType.parent.collapsed === true) {
                return false;
            }
            documentedType = documentedType.parent;
        }
        
        return true;
    };
    
    /**
     * Determines if the specified item is an ancestor.
     * 
     * @param {type} item
     * @returns {Boolean}
     */
    var isAncestor = function (item) {
        return item.children.length > 0;
    };

    /**
     * Hides the selected controller service.
     */
    var clearSelectedControllerService = function () {
        $('#controller-service-type-description').text('');
        $('#controller-service-type-name').text('');
        $('#selected-controller-service-name').text('');
        $('#selected-controller-service-type').text('');
        $('#controller-service-description-container').hide();
    };
    
    /**
     * Clears the selected controller service type.
     */
    var clearControllerServiceSelection = function () {
        // clear the selected row
        clearSelectedControllerService();

        // clear the active cell the it can be reselected when its included
        var controllerServiceTypesGrid = $('#controller-service-types-table').data('gridInstance');
        controllerServiceTypesGrid.resetActiveCell();
    };
    
    /**
     * Performs the filtering.
     * 
     * @param {object} item     The item subject to filtering
     * @param {object} args     Filter arguments
     * @returns {Boolean}       Whether or not to include the item
     */
    var filterControllerServiceTypes = function (item, args) {
        if (!areAncestorsExpanded(item)) {
            // if this item is currently selected and its parent is not collapsed
            if ($('#selected-controller-service-type').text() === item['type']) {
                clearControllerServiceSelection();
            }
            
            // update visibility flag
            item.visible = false;
            
            return false;
        }
        
        // don't allow ancestors to be filtered out (unless any of their ancestors
        // are collapsed)
        if (isAncestor(item)) {
            // update visibility flag
            item.visible = false;
            
            return true;
        }
        
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
            clearControllerServiceSelection();
        }
        
        // update visibility flag
        item.visible = matches;
        
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
     * Formats the type by introducing expand/collapse where appropriate.
     * 
     * @param {type} row
     * @param {type} cell
     * @param {type} value
     * @param {type} columnDef
     * @param {type} dataContext
     */
    var typeFormatter = function (row, cell, value, columnDef, dataContext) {
        var markup = '';
        
        var indent = 0;
        var documentedType = dataContext;
        while (documentedType.parent !== null) {
            indent += 10;
            documentedType = documentedType.parent;
        }
        
        var padding = 3;
        
        // create the markup for the row
        if (dataContext.children.length > 0) {
            // determine how to render the expansion button
            var expansionStyle = 'expanded';
            if (dataContext.collapsed === true) {
                expansionStyle = 'collapsed';
            }
            
            // to calculate the number of visible children
            var visibleChildren = 0;
            $.each(dataContext.children, function(_, child) {
                if (child.visible) {
                    visibleChildren++;
                }
            });
            
            markup += ('<span style="margin-top: 5px; margin-left: ' + indent + 'px;" class="expansion-button ' + expansionStyle + '"></span><span class="ancestor-type" style="margin-left: ' + padding + 'px;">' + value + '</span><span class="ancestor-type-rollup">(' + visibleChildren + ' of ' + dataContext.children.length + ')</span>');
        } else {
            if (dataContext.parent === null) {
                padding = 0;
            }
            
            markup += ('<span style="margin-left: ' + (indent + padding) + 'px;">' + value + '</span>');
        }
        
        return markup;
    };

    /**
     * Adds a new controller service of the specified type.
     * 
     * @param {string} controllerServiceType
     */
    var addControllerService = function (controllerServiceType) {
        var revision = nf.Client.getRevision();

        // add the new controller service
        var addService = $.ajax({
            type: 'POST',
            url: config.urls.controllerServices,
            data: {
                version: revision.version,
                clientId: revision.clientId,
                type: controllerServiceType
            },
            dataType: 'json'
        }).done(function (response) {
            // update the revision
            nf.Client.setRevision(response.revision);

            var controllerServicesGrid = $('#controller-services-table').data('gridInstance');
            var controllerServicesData = controllerServicesGrid.getData();
            controllerServicesData.addItem(response.controllerService);
        });
        
        // hide the dialog
        $('#new-controller-service-dialog').modal('hide');
        
        return addService;
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
        
        // define the function for filtering the list
        $('#controller-service-type-filter').keyup(function () {
            applyControllerServiceTypeFilter();
        }).focus(function () {
            if ($(this).hasClass(config.styles.filterList)) {
                $(this).removeClass(config.styles.filterList).val('');
            }
        }).blur(function () {
            if ($(this).val() === '') {
                $(this).addClass(config.styles.filterList).val(config.filterText);
            }
        }).addClass(config.styles.filterList).val(config.filterText);

        // initialize the processor type table
        var controllerServiceTypesColumns = [
            {id: 'type', name: 'Type', field: 'label', formatter: typeFormatter, sortable: false, resizable: true},
            {id: 'tags', name: 'Tags', field: 'tags', sortable: false, resizable: true}
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
        controllerServiceTypesData.getItemMetadata = function (index) {
            var item = controllerServiceTypesData.getItem(index);
            if (item && item.children.length > 0) {
                return {
                    selectable: false,
                    columns: {
                        0: {
                            colspan: '*'
                        }
                    }
                };
            } else {
                return {};
            }
        };
        
        var getVisibleControllerServiceCount = function () {
            var count = 0;
            for (var i = 0; i < controllerServiceTypesData.getLength(); i++) {
                var item = controllerServiceTypesData.getItem(i);
                if (item.children.length === 0) {
                    count++;
                }
            }
            return count;
        };

        // initialize the grid
        var controllerServiceTypesGrid = new Slick.Grid('#controller-service-types-table', controllerServiceTypesData, controllerServiceTypesColumns, controllerServiceTypesOptions);
        controllerServiceTypesGrid.setSelectionModel(new Slick.RowSelectionModel());
        controllerServiceTypesGrid.registerPlugin(new Slick.AutoTooltips());
        controllerServiceTypesGrid.setSortColumn('type', true);
        controllerServiceTypesGrid.onSelectedRowsChanged.subscribe(function (e, args) {
            if ($.isArray(args.rows) && args.rows.length === 1) {
                var controllerServiceTypeIndex = args.rows[0];
                var controllerServiceType = controllerServiceTypesGrid.getDataItem(controllerServiceTypeIndex);

                // only allow selection of service implementations
                if (controllerServiceType.children.length === 0) {
                    // set the controller service type description
                    if (nf.Common.isBlank(controllerServiceType.description)) {
                        $('#controller-service-type-description').attr('title', '').html('<span class="unset">No description specified</span>');
                    } else {
                        $('#controller-service-type-description').text(controllerServiceType.description).ellipsis();
                    }

                    // populate the dom
                    $('#controller-service-type-name').text(controllerServiceType.label).ellipsis();
                    $('#selected-controller-service-name').text(controllerServiceType.label);
                    $('#selected-controller-service-type').text(controllerServiceType.type);
                    
                    // show the selected controller service
                    $('#controller-service-description-container').show();
                }
            }
        });
        controllerServiceTypesGrid.onClick.subscribe(function (e, args) {
            var item = controllerServiceTypesData.getItem(args.row);
            if (item && item.children.length > 0) {
                // update the grid
                item.collapsed = !item.collapsed;
                controllerServiceTypesData.updateItem(item.id, item);
                
                // prevent selection within slickgrid
                e.stopImmediatePropagation();
            }
        });
        controllerServiceTypesGrid.onDblClick.subscribe(function (e, args) {
            var controllerServiceType = controllerServiceTypesGrid.getDataItem(args.row);
            addControllerService(controllerServiceType.type);
        });

        // wire up the dataview to the grid
        controllerServiceTypesData.onRowCountChanged.subscribe(function (e, args) {
            controllerServiceTypesGrid.updateRowCount();
            controllerServiceTypesGrid.render();

            // update the total number of displayed processors
            $('#displayed-controller-service-types').text(getVisibleControllerServiceCount());
        });
        controllerServiceTypesData.onRowsChanged.subscribe(function (e, args) {
            controllerServiceTypesGrid.invalidateRows(args.rows);
            controllerServiceTypesGrid.render();
        });
        controllerServiceTypesData.syncGridSelection(controllerServiceTypesGrid, true);

        // hold onto an instance of the grid
        $('#controller-service-types-table').data('gridInstance', controllerServiceTypesGrid);
        
        // load the available controller services
        $.ajax({
            type: 'GET',
            url: config.urls.controllerServiceTypes,
            dataType: 'json'
        }).done(function(response) {
            var id = 0;
            var tags = [];

            // begin the update
            controllerServiceTypesData.beginUpdate();

            var addType = function (parentItem, documentedType) {
                var item = {
                    id: id++,
                    label: nf.Common.substringAfterLast(documentedType.type, '.'),
                    type: documentedType.type,
                    description: nf.Common.escapeHtml(documentedType.description),
                    tags: documentedType.tags.join(', '),
                    parent: parentItem,
                    children: [],
                    collapsed: false,
                    visible: true
                };
                
                // add the documented type
                controllerServiceTypesData.addItem(item);
                
                // count the frequency of each tag for this type
                $.each(documentedType.tags, function (i, tag) {
                    tags.push(tag.toLowerCase());
                });
                
                // add each of its children
                $.each(documentedType.childTypes, function (_, documentedChildType) {
                    var childItem = addType(item, documentedChildType);
                    item.children.push(childItem);
                });
                
                return item;
            };

            // go through each controller service type
            $.each(response.controllerServiceTypes, function (i, documentedType) {
                addType(null, documentedType);
            });

            // end the udpate
            controllerServiceTypesData.endUpdate();

            // set the total number of processors
            $('#total-controller-service-types, #displayed-controller-service-types').text(getVisibleControllerServiceCount());

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
                        var selectedServiceType = $('#selected-controller-service-type').text();
                        addControllerService(selectedServiceType);
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
                clearSelectedControllerService();

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
            return '<img src="images/iconDetails.png" title="View Details" class="pointer view-controller-service" style="margin-top: 5px; float: left;" />';
        };
        
        var typeFormatter = function (row, cell, value, columnDef, dataContext) {
            return nf.Common.substringAfterLast(value, '.');
        };
        
        var enabledFormatter = function (row, cell, value, columnDef, dataContext) {
            if (dataContext.enabled === true) {
                return 'Enabled';
            } else {
                return 'Disabled';
            }
        };
        
        // define the column model for the controller services table
        var controllerServicesColumns = [
            {id: 'moreDetails', name: '&nbsp;', resizable: false, formatter: moreControllerServiceDetails, sortable: false, width: 50, maxWidth: 50},
            {id: 'name', field: 'name', name: 'Name', sortable: true, resizable: true},
            {id: 'type', field: 'type', name: 'Type', formatter: typeFormatter, sortable: true, resizable: true},
            {id: 'enabled', field: 'enabled', name: 'State', formatter: enabledFormatter, sortable: true, resizeable: true}
        ];
        
        // only DFM can edit controller services
        if (nf.Common.isDFM()) {
            var controllerServiceActionFormatter = function (row, cell, value, columnDef, dataContext) {
                var markup = '';

                if (dataContext.enabled === true) {
                    markup += '<img src="images/iconDisable.png" title="Disable" class="pointer disable-controller-service" style="margin-top: 2px;" />&nbsp;';
                } else {
                    markup += '<img src="images/iconEdit.png" title="Edit" class="pointer edit-controller-service" style="margin-top: 2px;" />&nbsp;<img src="images/iconRun.png" title="Enable" class="pointer enable-controller-service" style="margin-top: 2px;"/>&nbsp;<img src="images/iconDelete.png" title="Remove" class="pointer delete-controller-service" style="margin-top: 2px;" />&nbsp;';
                }

                return markup;
            };
            
            controllerServicesColumns.push({id: 'actions', name: '&nbsp;', resizable: false, formatter: controllerServiceActionFormatter, sortable: false, width: 75, maxWidth: 75});
        }
        
        var controllerServicesOptions = {
            forceFitColumns: true,
            enableTextSelectionOnCells: true,
            enableCellNavigation: true,
            enableColumnReorder: false,
            autoEdit: false,
            multiSelect: false
        };

        // initialize the dataview
        var controllerServicesData = new Slick.Data.DataView({
            inlineFilters: false
        });
        controllerServicesData.setItems([]);
        
        // initialize the grid
        var controllerServicesGrid = new Slick.Grid('#controller-services-table', controllerServicesData, controllerServicesColumns, controllerServicesOptions);
        controllerServicesGrid.setSelectionModel(new Slick.RowSelectionModel());
        controllerServicesGrid.registerPlugin(new Slick.AutoTooltips());
        controllerServicesGrid.setSortColumn('name', true);
        
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
                controllerServicesData.updateItem(controllerService.id, response.controllerService);
            }).fail(nf.Common.handleAjaxError);
        };
        
        // configure a click listener
        controllerServicesGrid.onClick.subscribe(function (e, args) {
            var target = $(e.target);
            
            // get the service at this row
            var controllerService = controllerServicesData.getItem(args.row);
            
            // determine the desired action
            if (controllerServicesGrid.getColumns()[args.cell].id === 'actions') {
                if (target.hasClass('edit-controller-service')) {
                    nf.ControllerServiceConfiguration.showConfiguration(controllerService);
                } else if (target.hasClass('enable-controller-service')) {
                    setEnabled(controllerService, true);
                } else if (target.hasClass('disable-controller-service')) {
                    setEnabled(controllerService, false);
                } else if (target.hasClass('delete-controller-service')) {
                    var revision = nf.Client.getRevision();
                    return $.ajax({
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
                        controllerServicesData.deleteItem(controllerService.id);
                    }).fail(nf.Common.handleAjaxError);
                }
            } else if (controllerServicesGrid.getColumns()[args.cell].id === 'moreDetails') {
                if (target.hasClass('view-controller-service')) {
                    
                }
            }
        });

        // wire up the dataview to the grid
        controllerServicesData.onRowCountChanged.subscribe(function (e, args) {
            controllerServicesGrid.updateRowCount();
            controllerServicesGrid.render();
        });
        controllerServicesData.onRowsChanged.subscribe(function (e, args) {
            controllerServicesGrid.invalidateRows(args.rows);
            controllerServicesGrid.render();
        });
        controllerServicesData.syncGridSelection(controllerServicesGrid, true);

        // hold onto an instance of the grid
        $('#controller-services-table').data('gridInstance', controllerServicesGrid);
    };
    
    /**
     * Loads the controller services.
     */
    var loadControllerServices = function () {
        return $.ajax({
            type: 'GET',
            url: config.urls.controllerServices,
            dataType: 'json'
        }).done(function (response) {
            var controllerServices = response.controllerServices;
            if (nf.Common.isDefinedAndNotNull(controllerServices)) {
                var controllerServicesGrid = $('#controller-services-table').data('gridInstance');
                var controllerServicesData = controllerServicesGrid.getData();

                // update the processors
                controllerServicesData.setItems(controllerServices);
                controllerServicesData.reSort();
                controllerServicesGrid.invalidate();
            }
        });
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
        });
        
    };
    
    /**
     * Initializes the reporting tasks tab.
     */
    var initReportingTasks = function () {
        // initialize the new reporting task dialog
        initNewReportingTaskDialog();
        
        var moreReportingTaskDetails = function (row, cell, value, columnDef, dataContext) {
            return '<img src="images/iconDetails.png" title="View Details" class="pointer view-reporting-task" style="margin-top: 5px; float: left;" />';
        };
        
        // define the column model for the reporting tasks table
        var reportingTasksColumnModel = [
            {id: 'moreDetails', field: 'moreDetails', name: '&nbsp;', resizable: false, formatter: moreReportingTaskDetails, sortable: true, width: 50, maxWidth: 50},
            {id: 'name', field: 'name', name: 'Name', sortable: true, resizable: true},
            {id: 'type', field: 'type', name: 'Type', sortable: true, resizable: true}
        ];
    };
    
    /**
     * Loads the reporting tasks.
     */
    var loadReportingTasks = function () {
        
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
                        
                        // resize the table
                        nf.Settings.resetTableSize();
                    }
                }
            });
            
            // setup the tooltip for the refresh icon
            $('#settings-refresh-required-icon').qtip($.extend({
                content: 'This flow has been modified by another user. Please refresh.'
            }, nf.CanvasUtils.config.systemTooltipConfig));
            
            // refresh the system diagnostics when clicked
            nf.Common.addHoverEffect('#settings-refresh-button', 'button-refresh', 'button-refresh-hover').click(function () {
                if ($('#settings-refresh-required-icon').is(':visible')) {
                    nf.CanvasHeader.reloadAndClearWarnings();
                } else {
                    nf.Settings.loadSettings();
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
         * Shows the settings dialog.
         */
        showSettings: function () {
            return nf.Settings.loadSettings().done(function () {
                // show the settings dialog
                nf.Shell.showContent('#settings').done(function () {
                    // reset button state
                    $('#settings-save').mouseout();
                });
            });
        },
        
        /**
         * Loads the settings.
         */
        loadSettings: function () {
            var settings = $.ajax({
                type: 'GET',
                url: config.urls.controllerConfig,
                dataType: 'json'
            }).done(function (response) {
                // ensure the config is present
                if (nf.Common.isDefinedAndNotNull(response.config)) {
                    // set the header
                    $('#settings-header-text').text(response.config.name + ' Settings');
                    $('#settings-last-refreshed').text(response.config.currentTime);

                    // populate the controller settings
                    $('#data-flow-title-field').val(response.config.name);
                    $('#data-flow-comments-field').val(response.config.comments);
                    $('#maximum-timer-driven-thread-count-field').val(response.config.maxTimerDrivenThreadCount);
                    $('#maximum-event-driven-thread-count-field').val(response.config.maxEventDrivenThreadCount);
                }
            });
            
            // load the controller services
            var controllerServices = loadControllerServices();
            
            // load the reporting tasks
            var reportingTasks = loadReportingTasks();
            
            // return a deferred for all parts of the settings
            return $.when(settings, controllerServices, reportingTasks).done(function () {
                
            }).fail(nf.Common.handleAjaxError);
        }
    };
}());