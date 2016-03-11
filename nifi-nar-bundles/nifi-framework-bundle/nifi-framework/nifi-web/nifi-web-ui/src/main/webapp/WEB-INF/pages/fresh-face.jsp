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
<!DOCTYPE html>
<html>
    <head>
        <title>NiFi</title>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
        <link rel="shortcut icon" href="images/nifi16.ico"/>
        <link rel="stylesheet" href="css/reset.css" type="text/css" />
        <link rel="stylesheet" href="css/fresh-face.css" type="text/css" />
    </head>
    <body>
        <div id="toolbar-container">
            <div id="logo">logo</div>
            <div id="toolbox">toolbox</div>
            <div id="global-administer">administer</div>
            <div class="clear"></div>
        </div>
        <div id="navigation-container">
            <div id="flow-status">flow status</div>
            <div id="search">
                <input id="search-field" placeholder="Search" />
            </div>
            <div id="controller-bulletins">&dotsquare;</div>
            <div class="clear"></div>
        </div>
        <div id="breadcrumbs-container">
            <div id="breadcrumbs">breadcrumbs</div>
        </div>
        <div id="action-panels">
            <div id="navigate-panel" class="action-panel">N</div>
            <div id="act-panel" class="action-panel">A</div>
        </div>
    </body>
</html>