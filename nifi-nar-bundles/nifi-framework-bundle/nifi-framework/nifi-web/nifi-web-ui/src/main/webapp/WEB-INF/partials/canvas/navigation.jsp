<%--
 Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
--%>
<%@ page contentType="text/html" pageEncoding="UTF-8" session="false" %>
<div id="breadcrumbs">
    <div id="cluster-indicator"></div>
    <div id="data-flow-title-viewport">
        <div id="breadcrumbs-left-border"></div>
        <div id="data-flow-title-container"></div>
        <div id="breadcrumbs-right-border"></div>
    </div>
    <div id="breadcrumbs-background"></div>
</div>
<div id="graph-controls">
    <div id="navigation-control" class="graph-control">
        <div class="graph-control-docked pointer" title="Navigate">
            <i class="graph-control-icon nifi-icon icon-compass"></i>
        </div>
        <div class="graph-control-header-container hidden">
            <div class="graph-control-header-icon">
                <i class="graph-control-icon nifi-icon icon-compass"></i>
            </div>
            <div class="graph-control-header">NAVIGATE</div>
            <div class="graph-control-header-action">
                <i class="graph-control-expansion nifi-icon icon-plus-squared-alt pointer"></i>
            </div>
            <div class="clear"></div>
        </div>
        <div class="graph-control-content hidden">
            <div id="navigation-buttons">
                <div id="naviagte-zoom-in" class="action-button" title="Zoom In">
                    <i class="graph-control-action-icon nifi-icon icon-zoom-in"></i>
                </div>
                <div class="button-spacer-small">&nbsp;</div>
                <div id="naviagte-zoom-out" class="action-button" title="Zoom Out">
                    <i class="graph-control-action-icon nifi-icon icon-zoom-out"></i>
                </div>
                <div class="button-spacer-large">&nbsp;</div>
                <div id="naviagte-zoom-fit" class="action-button" title="Fit">
                    <i class="graph-control-action-icon nifi-icon icon-fighter-jet"></i>
                </div>
                <div class="button-spacer-small">&nbsp;</div>
                <div id="naviagte-zoom-actual-size" class="action-button" title="Actual">
                    <i class="graph-control-action-icon nifi-icon icon-award"></i>
                </div>
                <div class="clear"></div>
            </div>
            <div id="birdseye"></div>
        </div>
    </div>
    <div id="operation-control" class="graph-control">
        <div class="graph-control-docked pointer" title="Operate">
            <i class="graph-control-icon nifi-icon icon-bullseye"></i>
        </div>
        <div class="graph-control-header-container hidden">
            <div class="graph-control-header-icon">
                <i class="graph-control-icon nifi-icon icon-bullseye"></i>
            </div>
            <div class="graph-control-header">OPERATE</div>
            <div class="graph-control-header-action">
                <i class="graph-control-expansion nifi-icon icon-plus-squared-alt pointer"></i>
            </div>
            <div class="clear"></div>
        </div>
        <div class="graph-control-content hidden">
            <div id="operation-buttons">
                <div>
                    <div id="operate-enable" class="action-button" title="Enable">
                        <i class="graph-control-action-icon nifi-icon icon-toggle-on"></i>
                    </div>
                    <div class="button-spacer-small">&nbsp;</div>
                    <div id="operate-disable" class="action-button" title="Disable">
                        <i class="graph-control-action-icon nifi-icon icon-toggle-off"></i>
                    </div>
                    <div class="button-spacer-large">&nbsp;</div>
                    <div id="operate-start" class="action-button" title="Start">
                        <i class="graph-control-action-icon nifi-icon icon-play"></i>
                    </div>
                    <div class="button-spacer-small">&nbsp;</div>
                    <div id="operate-stop" class="action-button" title="Stop">
                        <i class="graph-control-action-icon nifi-icon icon-stop"></i>
                    </div>
                    <div class="button-spacer-large">&nbsp;</div>
                    <div id="operate-template" class="action-button" title="Create Template">
                        <i class="graph-control-action-icon nifi-icon icon-bus"></i>
                    </div>
                    <div class="button-spacer-large">&nbsp;</div>
                    <div id="operate-copy" class="action-button" title="Copy">
                        <i class="graph-control-action-icon nifi-icon icon-th"></i>
                    </div>
                    <div class="button-spacer-small">&nbsp;</div>
                    <div id="operate-paste" class="action-button" title="Paste">
                        <i class="graph-control-action-icon nifi-icon icon-paste"></i>
                    </div>
                    <div class="clear"></div>
                </div>
                <div style="margin-top: 5px;">
                    <div id="operate-group" class="action-button" title="Group">
                        <i class="graph-control-action-icon nifi-icon icon-fire"></i>
                    </div>
                    <div class="button-spacer-large">&nbsp;</div>
                    <div id="operate-color" class="action-button" title="Fill Color">
                        <i class="graph-control-action-icon nifi-icon icon-brush-1"></i>
                    </div>
                    <div class="button-spacer-large">&nbsp;</div>
                    <div id="operate-delete" class="action-button" title="Fill Color">
                        <i class="graph-control-action-icon nifi-icon icon-trash-empty"></i><span id="operate-delete-text">DELETE</span>
                    </div>
                    <div class="clear"></div>
                </div>
            </div>
        </div>
    </div>
</div>
    <%--<div id="zoom-in-button" title="Zoom In" class="zoom-in"></div>--%>
    <%--<div id="zoom-out-button" title="Zoom Out" class="zoom-out"></div>--%>
    <%--<div id="zoom-fit-button" title="Fit" class="fit-image"></div>--%>
    <%--<div id="zoom-actual-button" title="Actual Size" class="actual-size"></div>--%>