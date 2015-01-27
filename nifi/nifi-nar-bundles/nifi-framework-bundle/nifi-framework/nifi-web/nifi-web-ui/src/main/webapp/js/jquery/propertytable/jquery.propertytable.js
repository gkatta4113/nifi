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

/**
 * Create a property table. The options are specified in the following
 * format:
 *
 * {
 *   tags: ['attributes', 'copy', 'regex', 'xml', 'copy', 'xml', 'attributes'],
 *   select: selectHandler,
 *   remove: removeHandler
 * }
 */

/**
 * jQuery plugin for a property table.
 * 
 * @param {type} $
 */
(function ($) {
    var languageId = 'nfel';
    var editorClass = languageId + '-editor';

    var isUndefined = function (obj) {
        return typeof obj === 'undefined';
    };

    var isNull = function (obj) {
        return obj === null;
    };

    var isDefinedAndNotNull = function (obj) {
        return !isUndefined(obj) && !isNull(obj);
    };

    var isBlank = function (str) {
        return isUndefined(str) || isNull(str) || str === '';
    };

    var initPropertiesTable = function (table, options) {
        // function for formatting the property name
        var nameFormatter = function (row, cell, value, columnDef, dataContext) {
            var nameWidthOffset = 10;
            var cellContent = $('<div></div>');

            // format the contents
            var formattedValue = $('<span/>').addClass('table-cell').text(value).appendTo(cellContent);
            if (dataContext.type === 'required') {
                formattedValue.addClass('required');
            }

            // get the property descriptor
            var descriptors = table.data('descriptors');
            var propertyDescriptor = descriptors[dataContext.property];

            // show the property description if applicable
            if (nf.Common.isDefinedAndNotNull(propertyDescriptor)) {
                if (!nf.Common.isBlank(propertyDescriptor.description) || !nf.Common.isBlank(propertyDescriptor.defaultValue) || !nf.Common.isBlank(propertyDescriptor.supportsEl)) {
                    $('<img class="icon-info" src="images/iconInfo.png" alt="Info" title="" style="float: right; margin-right: 6px; margin-top: 4px;" />').appendTo(cellContent);
                    $('<span class="hidden property-descriptor-name"></span>').text(dataContext.property).appendTo(cellContent);
                    nameWidthOffset = 26; // 10 + icon width (10) + icon margin (6)
                }
            }

            // adjust the width accordingly
            formattedValue.width(columnDef.width - nameWidthOffset).ellipsis();

            // return the cell content
            return cellContent.html();
        };

        // function for formatting the property value
        var valueFormatter = function (row, cell, value, columnDef, dataContext) {
            var valueMarkup;
            if (nf.Common.isDefinedAndNotNull(value)) {
                // get the property descriptor
                var descriptors = table.data('descriptors');
                var propertyDescriptor = descriptors[dataContext.property];

                // determine if the property is sensitive
                if (nf.Common.isSensitiveProperty(propertyDescriptor)) {
                    valueMarkup = '<span class="table-cell sensitive">Sensitive value set</span>';
                } else {
                    // if there are allowable values, attempt to swap out for the display name
                    var allowableValues = nf.Common.getAllowableValues(propertyDescriptor);
                    if ($.isArray(allowableValues)) {
                        $.each(allowableValues, function (_, allowableValue) {
                            if (value === allowableValue.value) {
                                value = allowableValue.displayName;
                                return false;
                            }
                        });
                    }

                    if (value === '') {
                        valueMarkup = '<span class="table-cell blank">Empty string set</span>';
                    } else {
                        valueMarkup = '<div class="table-cell value"><pre class="ellipsis">' + nf.Common.escapeHtml(value) + '</pre></div>';
                    }
                }
            } else {
                valueMarkup = '<span class="unset">No value set</span>';
            }

            // format the contents
            var content = $(valueMarkup);
            if (dataContext.type === 'required') {
                content.addClass('required');
            }
            content.find('.ellipsis').width(columnDef.width - 10).ellipsis();

            // return the appropriate markup
            return $('<div/>').append(content).html();
        };

        var processorConfigurationColumns = [
            {id: 'property', field: 'displayName', name: 'Property', sortable: false, resizable: true, rerenderOnResize: true, formatter: nameFormatter},
            {id: 'value', field: 'value', name: 'Value', sortable: false, resizable: true, cssClass: 'pointer', rerenderOnResize: true, formatter: valueFormatter}
        ];
        
        if (options.readOnly !== true) {
            // custom formatter for the actions column
            var actionFormatter = function (row, cell, value, columnDef, dataContext) {
                var markup = '';

                // allow user defined properties to be removed
                if (dataContext.type === 'userDefined') {
                    markup = '<img src="images/iconDelete.png" title="Delete" class="delete-property pointer" style="margin-top: 2px" />';
                }

                return markup;
            };
            processorConfigurationColumns.push({id: "actions", name: "&nbsp;", minWidth: 20, width: 20, formatter: actionFormatter});
        }
        
        var propertyConfigurationOptions = {
            forceFitColumns: true,
            enableTextSelectionOnCells: true,
            enableCellNavigation: true,
            enableColumnReorder: false,
            editable: true,
            enableAddRow: false,
            autoEdit: false
        };

        // initialize the dataview
        var propertyData = new Slick.Data.DataView({
            inlineFilters: false
        });
        propertyData.setItems([]);
        propertyData.setFilterArgs({
            searchString: '',
            property: 'hidden'
        });
        propertyData.setFilter(filter);
        propertyData.getItemMetadata = function (index) {
            var item = propertyData.getItem(index);

            // get the property descriptor
            var descriptors = table.data('descriptors');
            var propertyDescriptor = descriptors[item.property];

            // support el if specified or unsure yet (likely a dynamic property)
            if (nf.Common.isUndefinedOrNull(propertyDescriptor) || nf.Common.supportsEl(propertyDescriptor)) {
                return {
                    columns: {
                        value: {
                            editor: nf.ProcessorPropertyNfelEditor
                        }
                    }
                };
            } else {
                // check for allowable values which will drive which editor to use
                var allowableValues = nf.Common.getAllowableValues(propertyDescriptor);
                if ($.isArray(allowableValues)) {
                    return {
                        columns: {
                            value: {
                                editor: nf.ProcessorPropertyComboEditor
                            }
                        }
                    };
                } else {
                    return {
                        columns: {
                            value: {
                                editor: nf.ProcessorPropertyTextEditor
                            }
                        }
                    };
                }
            }
        };

        // initialize the grid
        var propertyGrid = new Slick.Grid(table, propertyData, processorConfigurationColumns, propertyConfigurationOptions);
        propertyGrid.setSelectionModel(new Slick.RowSelectionModel());
        propertyGrid.onClick.subscribe(function (e, args) {
            if (propertyGrid.getColumns()[args.cell].id === 'value') {
                // edits the clicked cell
                propertyGrid.gotoCell(args.row, args.cell, true);

                // prevents standard edit logic
                e.stopImmediatePropagation();
            } else if (propertyGrid.getColumns()[args.cell].id === 'actions') {
                var target = $(e.target);
                if (target.hasClass('delete-property')) {
                    // mark the property in question for removal
                    var property = propertyData.getItem(args.row);
                    property.hidden = true;

                    // refresh the table
                    propertyData.updateItem(property.id, property);
                    
                    // prevents standard edit logic
                    e.stopImmediatePropagation();
                }
            }
        });

        // wire up the dataview to the grid
        propertyData.onRowCountChanged.subscribe(function (e, args) {
            propertyGrid.updateRowCount();
            propertyGrid.render();
        });
        propertyData.onRowsChanged.subscribe(function (e, args) {
            propertyGrid.invalidateRows(args.rows);
            propertyGrid.render();
        });

        // hold onto an instance of the grid and listen for mouse events to add tooltips where appropriate
        table.data('gridInstance', propertyGrid).on('mouseenter', 'div.slick-cell', function (e) {
            var infoIcon = $(this).find('img.icon-info');
            if (infoIcon.length && !infoIcon.data('qtip')) {
                var property = $(this).find('span.property-descriptor-name').text();

                // get the property descriptor
                var descriptors = table.data('descriptors');
                var propertyDescriptor = descriptors[property];

                // get the processor history
                var history = table.data('history');
                var propertyHistory = history[property];

                // format the tooltip
                var tooltip = nf.Common.formatPropertyTooltip(propertyDescriptor, propertyHistory);

                if (nf.Common.isDefinedAndNotNull(tooltip)) {
                    infoIcon.qtip($.extend({
                        content: tooltip
                    }, nf.Common.config.tooltipConfig));
                }
            }
        });
    };
    
    var saveRow = function (table) {
        // get the property grid to commit the current edit
        var propertyGrid = table.data('gridInstance');
        if (nf.Common.isDefinedAndNotNull(propertyGrid)) {
            var editController = propertyGrid.getEditController();
            editController.commitCurrentEdit();
        }
    };
    
    /**
     * Performs the filtering.
     * 
     * @param {object} item     The item subject to filtering
     * @param {object} args     Filter arguments
     * @returns {Boolean}       Whether or not to include the item
     */
    var filter = function (item, args) {
        return item.hidden === false;
    };
    
    /**
     * Loads the specified properties.
     * 
     * @param {type} table 
     * @param {type} properties
     * @param {type} descriptors
     * @param {type} history
     */
    var loadProperties = function (table, properties, descriptors, history) {
        // save the descriptors and history
        table.data({
            'descriptors': descriptors,
            'history': history
        });
        
        // get the grid
        var propertyGrid = table.data('gridInstance');
        var propertyData = propertyGrid.getData();

        // generate the properties
        if (nf.Common.isDefinedAndNotNull(properties)) {
            propertyData.beginUpdate();

            var i = 0;
            $.each(properties, function (name, value) {
                // get the property descriptor
                var descriptor = descriptors[name];

                // determine the property type
                var type = 'userDefined';
                var displayName = name;
                if (nf.Common.isDefinedAndNotNull(descriptor)) {
                    if (nf.Common.isRequiredProperty(descriptor)) {
                        type = 'required';
                    } else if (nf.Common.isDynamicProperty(descriptor)) {
                        type = 'userDefined';
                    } else {
                        type = 'optional';
                    }

                    // use the display name if possible
                    displayName = descriptor.displayName;

                    // determine the value
                    if (nf.Common.isNull(value) && nf.Common.isDefinedAndNotNull(descriptor.defaultValue)) {
                        value = descriptor.defaultValue;
                    }
                }

                // add the row
                propertyData.addItem({
                    id: i++,
                    hidden: false,
                    property: name,
                    displayName: displayName,
                    previousValue: value,
                    value: value,
                    type: type
                });
            });

            propertyData.endUpdate();
        }
    };

    var methods = {
        
        /**
         * Initializes the tag cloud.
         * 
         * @argument {object} options The options for the tag cloud
         */
        init: function (options) {
            return this.each(function () {
                // ensure the options have been properly specified
                if (isDefinedAndNotNull(options)) {
                    // get the tag cloud
                    var propertyTableContainer = $(this);
                    
                    // clear any current contents, remote events, and store options
                    propertyTableContainer.empty().unbind().data('options', options);

                    // build the component
                    var header = $('<div class="properties-header"></div>').appendTo(propertyTableContainer);
                    $('<div class="required-property-note">Required field</div>').appendTo(header);
                    
                    // build the table
                    var table = $('<div class="property-table"></div>').appendTo(propertyTableContainer);
                    
                    // optionally add a add new property button
                    if (options.readOnly !== true) {
                        // build the new property dialog
                        var newPropertyDialogMarkup = '<div class="new-property-dialog dialog">' +
                            '<div>' +
                                '<div class="setting-name">Property name</div>' +
                                '<div class="setting-field new-property-name-container">' +
                                    '<input class="new-property-name" type="text"/>' +
                                '</div>' +
                                '<div class="setting-name" style="margin-top: 36px;">Property value</div>' +
                                '<div class="setting-field">' +
                                    '<div class="new-property-value"></div>' +
                                '</div>' +
                            '</div>' +
                            '<div class="new-property-button-container">' +
                                '<div class="new-property-ok button button-normal">Ok</div>' +
                                '<div class="new-property-cancel button button-normal">Cancel</div>' +
                                '<div class="clear"></div>' +
                            '</div>' +
                        '</div>';

                        var newPropertyDialog = $(newPropertyDialogMarkup).appendTo(options.newPropertyDialogContainer);
                        var newPropertyNameField = newPropertyDialog.find('input.new-property-name');
                        var newPropertyValueField = newPropertyDialog.find('div.new-property-value');
                        
                        var add = function () {
                            var propertyName = $.trim(newPropertyNameField.val());
                            var propertyValue = newPropertyValueField.nfeditor('getValue');

                            // ensure the property name and value is specified
                            if (propertyName !== '') {
                                // add a row for the new property
                                var propertyGrid = table.data('gridInstance');
                                var propertyData = propertyGrid.getData();
                                propertyData.addItem({
                                    id: propertyData.getLength(),
                                    hidden: false,
                                    property: propertyName,
                                    displayName: propertyName,
                                    previousValue: null,
                                    value: propertyValue,
                                    type: 'userDefined'
                                });
                            } else {
                                nf.Dialog.showOkDialog({
                                    dialogContent: 'Property name must be specified.',
                                    overlayBackground: false
                                });
                            }

                            // close the dialog
                            newPropertyDialog.hide();
                        };

                        var cancel = function () {
                            newPropertyDialog.hide();
                        };
                        
                        // create the editor
                        newPropertyValueField.addClass(editorClass).nfeditor({
                            languageId: languageId,
                            width: 318,
                            minWidth: 318,
                            height: 106,
                            minHeight: 106,
                            resizable: true,
                            escape: cancel,
                            enter: add
                        });

                        // make the new property dialog draggable
                        newPropertyDialog.draggable({
                            cancel: 'input, textarea, pre, .button, .' + editorClass,
                            containment: 'body'
                        }).on('click', 'div.new-property-ok', add).on('click', 'div.new-property-cancel', cancel);

                        // enable tabs in the property value
                        newPropertyNameField.on('keydown', function (e) {
                            if (e.which === $.ui.keyCode.ENTER && !e.shiftKey) {
                                add();
                            } else if (e.which === $.ui.keyCode.ESCAPE) {
                                e.preventDefault();
                                cancel();
                            }
                        });
                        
                        // build the control to open the new property dialog
                        var addProperty = $('<div class="add-property"></div>').appendTo(header);
                        $('<div class="add-property-icon add-icon-bg"></div>').on('click', function() {
                            // close all fields currently being edited
                            saveRow(table);

                            // clear the dialog
                            newPropertyNameField.val('');
                            newPropertyValueField.nfeditor('setValue', '');

                            // reset the add property dialog position/size
                            newPropertyValueField.nfeditor('setSize', 318, 106);

                            // open the new property dialog
                            newPropertyDialog.center().show();

                            // give the property name focus
                            newPropertyValueField.nfeditor('refresh');
                            newPropertyNameField.focus();
                        }).on('mouseenter', function () {
                            $(this).removeClass('add-icon-bg').addClass('add-icon-bg-hover');
                        }).on('mouseleave', function () {
                            $(this).removeClass('add-icon-bg-hover').addClass('add-icon-bg');
                        }).appendTo(addProperty);
                        $('<div class="add-property-text">New property</div>').appendTo(addProperty);
                    }
                    $('<div class="clear"></div>').appendTo(header);
                    
                    // initializes the properties table
                    initPropertiesTable(table, options);
                }
            });
        },
        
        /**
         * Loads the specified properties.
         * 
         * @argument {object} properties        The properties
         * @argument {map} descriptors          The property descriptors (property name -> property descriptor)
         * @argument {map} history
         */
        loadProperties: function (properties, descriptors, history) {
            return this.each(function () {
                var table = $(this).find('div.property-table');
                loadProperties(table, properties, descriptors, history);
            });
        },
        
        /**
         * Saves the last edited row in the specified grid.
         */
        saveRow: function () {
            return this.each(function () {
                var table = $(this).find('div.property-table');
                saveRow(table);
            });
        },
        
        /**
         * Update the size of the grid based on its container's current size.
         */
        resetTableSize: function () {
            return this.each(function () {
                var table = $(this).find('div.property-table');
                var propertyGrid = table.data('gridInstance');
                if (nf.Common.isDefinedAndNotNull(propertyGrid)) {
                    propertyGrid.resizeCanvas();
                }
            });
        },
        
        /**
         * Cancels the edit in the specified row.
         */
        cancelEdit: function () {
            return this.each(function () {
                var table = $(this).find('div.property-table');
                var propertyGrid = table.data('gridInstance');
                if (nf.Common.isDefinedAndNotNull(propertyGrid)) {
                    var editController = propertyGrid.getEditController();
                    editController.cancelCurrentEdit();
                }
            });
        },
        
        /**
         * Clears the property table.
         */
        clear: function () {
            return this.each(function () {
                var table = $(this).find('div.property-table');
            
                // clean up any tooltips that may have been generated
                nf.Common.cleanUpTooltips(table, 'img.icon-info');

                // clear the data in the grid
                var propertyGrid = table.data('gridInstance');
                var propertyData = propertyGrid.getData();
                propertyData.setItems([]);
            });
        },
        
        /**
         * Determines if a save is required for the first matching element.
         */
        isSaveRequired: function () {
            var isSaveRequired = false;
            
            this.each(function () {
                // get the property grid
                var table = $(this).find('div.property-table');
                var propertyGrid = table.data('gridInstance');
                var propertyData = propertyGrid.getData();

                // determine if any of the processor properties have changed
                $.each(propertyData.getItems(), function () {
                    if (this.value !== this.previousValue) {
                        isSaveRequired = true;
                        return false;
                    }
                });
                
                return false;
            });
            
            return isSaveRequired;
        },
        
        /**
         * Marshalls the properties for the first matching element.
         */
        marshalProperties: function () {
            // properties
            var properties = {};

            this.each(function () {
                // get the property grid data
                var table = $(this).find('div.property-table');
                var propertyGrid = table.data('gridInstance');
                var propertyData = propertyGrid.getData();
                $.each(propertyData.getItems(), function () {
                    if (this.hidden === true) {
                        properties[this.property] = null;
                    } else if (this.value !== this.previousValue) {
                        properties[this.property] = this.value;
                    }
                });
                
                return false;
            });

            return properties;
        }
    };

    $.fn.propertytable = function (method) {
        if (methods[method]) {
            return methods[method].apply(this, Array.prototype.slice.call(arguments, 1));
        } else {
            return methods.init.apply(this, arguments);
        }
    };
})(jQuery);