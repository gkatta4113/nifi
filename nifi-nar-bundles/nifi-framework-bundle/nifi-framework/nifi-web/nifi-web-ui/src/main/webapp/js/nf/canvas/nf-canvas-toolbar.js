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

nf.CanvasToolbar = (function () {

    /**
     * Initializes the specified button and associates the specified action.
     *
     * @param button
     * @param action
     */
    var initializeButton = function (button, action) {
        button.on('click', function () {
            if (!$(this).hasClass('icon-disabled')) {
                // hide the context menu
                nf.ContextMenu.hide();

                // execute the action
                nf.Actions[action](nf.CanvasUtils.getSelection());
            }
        });
    };

    var enableButton = function (button) {
        button.addClass('pointer').find('i').removeClass('icon-disabled');
    };

    var disableButton = function (button) {
        button.removeClass('pointer').find('i').addClass('icon-disabled');
    };

    return {
        /**
         * Initializes the canvas toolbar.
         */
        init: function () {
            var disable = $('#operate-disable');
            var enable = $('#operate-enable');
            var start = $('#operate-start');
            var stop = $('#operate-stop');
            var template = $('#operate-template');
            var copy = $('#operate-copy');
            var paste = $('#operate-paste');
            var group = $('#operate-group');
            var color = $('#operate-color');

            // initialize the buttons
            initializeButton(disable, 'disable');
            initializeButton(enable, 'enable');
            initializeButton(start, 'start');
            initializeButton(stop, 'stop');
            initializeButton(template, 'template');
            initializeButton(copy, 'copy');
            initializeButton(paste, 'paste');
            initializeButton(group, 'group');
            initializeButton(color, 'fillColor');

            // set up initial states for selection-less items
            if (nf.Common.isDFM()) {
                enableButton(start);
                enableButton(stop);
                enableButton(template);
            } else {
                disableButton(start);
                disableButton(stop);
                disableButton(template);
            }

            // disable actions that require selection
            disableButton(enable);
            disableButton(disable);
            disableButton(copy);
            disableButton(paste);
            disableButton(color);
            disableButton(group);

            // add a clipboard listener if appropriate
            if (nf.Common.isDFM()) {
                nf.Clipboard.addListener(this, function (action, data) {
                    if (nf.Clipboard.isCopied()) {
                        enableButton(paste);
                    } else {
                        disableButton(paste);
                    }
                });
            }
        },
        
        /**
         * Called when the selection changes to update the toolbar appropriately.
         */
        refresh: function () {
            // only refresh the toolbar if DFM
            if (nf.Common.isDFM()) {
                var disable = $('#operate-disable');
                var enable = $('#operate-enable');
                var copy = $('#operate-copy');
                var group = $('#operate-group');
                var color = $('#operate-color');

                var selection = nf.CanvasUtils.getSelection();

                // if all selected components are deletable enable the delete button
                //if (!selection.empty()) {
                //    var enableDelete = true;
                //    selection.each(function (d) {
                //        if (!nf.CanvasUtils.isDeletable(d3.select(this))) {
                //            enableDelete = false;
                //            return false;
                //        }
                //    });
                //    if (enableDelete) {
                //        actions['delete'].enable();
                //    } else {
                //        actions['delete'].disable();
                //    }
                //} else {
                //    actions['delete'].disable();
                //}

                // if there are any copyable components enable the button
                if (nf.CanvasUtils.isCopyable(selection)) {
                    enableButton(copy);
                } else {
                    disableButton(copy);
                }

                // determine if the selection is groupable
                if (!selection.empty() && nf.CanvasUtils.isDisconnected(selection)) {
                    enableButton(group);
                } else {
                    disableButton(group);
                }

                // if there are any colorable components enable the fill button
                if (nf.CanvasUtils.isColorable(selection)) {
                    enableButton(color);
                } else {
                    disableButton(color);
                }
                
                // ensure the selection supports enable
                if (nf.CanvasUtils.canEnable(selection)) {
                    enableButton(enable);
                } else {
                    disableButton(enable);
                }

                // ensure the selection supports disable
                if (nf.CanvasUtils.canDisable(selection)) {
                    enableButton(disable);
                } else {
                    disableButton(disable);
                }
            }
        }
    };
}());