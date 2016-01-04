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

nf.GraphControl = (function () {

    var MIN_GRAPH_CONTROL_TOP = 117;

    var config = {
        translateIncrement: 20
    };

    /**
     * Opens the specified graph control.
     *
     * @param {jQuery} graphControl
     */
    var openGraphControl = function (graphControl) {
        // undock if necessary
        if ($('div.graph-control-content').is(':visible') === false) {
            $('#graph-controls div.graph-control-docked').hide();
            $('#graph-controls div.graph-control-header-container').show();
        }

        // show the content of the specified graph control
        graphControl.children('div.graph-control-content').show();
        graphControl.find('i.graph-control-expansion').removeClass('icon-plus-squared-alt').addClass('icon-minus-squared-alt');

        // handle specific actions as necessary
        if (graphControl.attr('id') === 'navigation-control') {
            nf.Birdseye.updateBirdseyeVisibility(true);
        }

        // get the current visibility
        var graphControlVisibility = nf.Storage.getItem('graph-control-visibility');
        if (graphControlVisibility === null) {
            graphControlVisibility = {};
        }

        // update the visibility for this graph control
        var graphControlId = graphControl.attr('id');
        graphControlVisibility[graphControlId] = true;
        nf.Storage.setItem('graph-control-visibility', graphControlVisibility);

        // reset the graph control position
        positionGraphControls();
    };

    /**
     * Hides the specified graph control.
     *
     * @param {jQuery} graphControl
     */
    var hideGraphControl = function (graphControl) {
        // hide the content of the specified graph control
        graphControl.children('div.graph-control-content').hide();
        graphControl.find('i.graph-control-expansion').removeClass('icon-minus-squared-alt').addClass('icon-plus-squared-alt');

        // dock if necessary
        if ($('div.graph-control-content').is(':visible') === false) {
            $('#graph-controls div.graph-control-header-container').hide();
            $('#graph-controls div.graph-control-docked').show();
        }

        // handle specific actions as necessary
        if (graphControl.attr('id') === 'navigation-control') {
            nf.Birdseye.updateBirdseyeVisibility(false);
        }

        // get the current visibility
        var graphControlVisibility = nf.Storage.getItem('graph-control-visibility');
        if (graphControlVisibility === null) {
            graphControlVisibility = {};
        }

        // update the visibility for this graph control
        var graphControlId = graphControl.attr('id');
        graphControlVisibility[graphControlId] = false;
        nf.Storage.setItem('graph-control-visibility', graphControlVisibility);

        // reset the graph control position
        positionGraphControls();
    };

    /**
     * Positions the graph controls based on the size of the screen.
     */
    var positionGraphControls = function () {
        var windowHeight = $(window).height();
        var navigationHeight = $('#navigation-control').outerHeight();
        var operationHeight = $('#operation-control').outerHeight();
        var graphControlTop = (windowHeight / 2) - ((navigationHeight + operationHeight) / 2);

        $('#graph-controls').css('top', Math.max(MIN_GRAPH_CONTROL_TOP, graphControlTop));
    };

    return {
        /**
         * Initializes the graph controls.
         */
        init: function () {

            // graph control undock
            $('#graph-controls').on('click', 'div.graph-control-docked', function () {
                openGraphControl($(this).parent());
            });

            // handle expansion
            $('#graph-controls').on('click', 'i.graph-control-expansion', function () {
                var icon = $(this);
                if (icon.hasClass('icon-plus-squared-alt')) {
                    openGraphControl(icon.closest('div.graph-control'));
                } else {
                    hideGraphControl(icon.closest('div.graph-control'));
                }
            });

            // zoom in
            $('#naviagte-zoom-in').on('click', function () {
                nf.Canvas.View.zoomIn();

                // hide the context menu
                nf.ContextMenu.hide();

                // refresh the canvas
                nf.Canvas.View.refresh({
                    transition: true
                });
            });

            // zoom out
            $('#naviagte-zoom-out').on('click', function () {
                nf.Canvas.View.zoomOut();

                // hide the context menu
                nf.ContextMenu.hide();

                // refresh the canvas
                nf.Canvas.View.refresh({
                    transition: true
                });
            });

            // zoom fit
            $('#naviagte-zoom-fit').on('click', function () {
                nf.Canvas.View.fit();

                // hide the context menu
                nf.ContextMenu.hide();

                // refresh the canvas
                nf.Canvas.View.refresh({
                    transition: true
                });
            });

            // one to one
            $('#naviagte-zoom-actual-size').on('click', function () {
                nf.Canvas.View.actualSize();

                // hide the context menu
                nf.ContextMenu.hide();

                // refresh the canvas
                nf.Canvas.View.refresh({
                    transition: true
                });
            });

            // initial the graph control visibility
            var graphControlVisibility = nf.Storage.getItem('graph-control-visibility');
            if (graphControlVisibility !== null) {
                $.each(graphControlVisibility, function (id, isVisible) {
                    var graphControl = $('#' + id);
                    if (graphControl) {
                        if (isVisible) {
                            openGraphControl(graphControl);
                        } else {
                            hideGraphControl(graphControl);
                        }
                    }
                });
            }

            // listen for browser resize events to reset the graph control positioning
            $(window).resize(positionGraphControls);

            // set the initial position
            positionGraphControls();
        }
    };
}());