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
package org.apache.nifi.controller.service;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.nifi.util.DomUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

/**
 *
 */
public class ControllerServiceLoader {

    private static final Log logger = LogFactory.getLog(ControllerServiceLoader.class);


    public static List<ControllerServiceNode> loadControllerServices(final ControllerServiceProvider provider, final InputStream serializedStream) throws IOException {
        final DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
        documentBuilderFactory.setNamespaceAware(true);

        final List<ControllerServiceNode> services = new ArrayList<>();

        try (final InputStream in = new BufferedInputStream(serializedStream)) {
            final DocumentBuilder builder = documentBuilderFactory.newDocumentBuilder();

            builder.setErrorHandler(new org.xml.sax.ErrorHandler() {

                @Override
                public void fatalError(final SAXParseException err) throws SAXException {
                    logger.error("Config file line " + err.getLineNumber() + ", col " + err.getColumnNumber() + ", uri " + err.getSystemId() + " :message: " + err.getMessage());
                    if (logger.isDebugEnabled()) {
                        logger.error("Error Stack Dump", err);
                    }
                    throw err;
                }

                @Override
                public void error(final SAXParseException err) throws SAXParseException {
                    logger.error("Config file line " + err.getLineNumber() + ", col " + err.getColumnNumber() + ", uri " + err.getSystemId() + " :message: " + err.getMessage());
                    if (logger.isDebugEnabled()) {
                        logger.error("Error Stack Dump", err);
                    }
                    throw err;
                }

                @Override
                public void warning(final SAXParseException err) throws SAXParseException {
                    logger.warn(" Config file line " + err.getLineNumber() + ", uri " + err.getSystemId() + " : message : " + err.getMessage());
                    if (logger.isDebugEnabled()) {
                        logger.warn("Warning stack dump", err);
                    }
                    throw err;
                }
            });

            //if controllerService.xml does not exist, create an empty file...
            final Document document = builder.parse(in);
            final Element controllerServices = DomUtils.getChild(document.getDocumentElement(), "controllerServices");
            final List<Element> serviceNodes = DomUtils.getChildElementsByTagName(controllerServices, "controllerService");
            for (final Element serviceElement : serviceNodes) {
                //get properties for the specific controller task - id, name, class,
                //and schedulingPeriod must be set
                final String serviceId = DomUtils.getChild(serviceElement, "id").getTextContent().trim();
                final String serviceClass = DomUtils.getChild(serviceElement, "class").getTextContent().trim();
                
                //set the class to be used for the configured controller task
                final ControllerServiceNode serviceNode = provider.createControllerService(serviceClass, serviceId, false);

                //optional task-specific properties
                for (final Element optionalProperty : DomUtils.getChildElementsByTagName(serviceElement, "property")) {
                    final String name = optionalProperty.getAttribute("name").trim();
                    final String value = optionalProperty.getTextContent().trim();
                    serviceNode.setProperty(name, value);
                }

                services.add(serviceNode);
                provider.enableControllerService(serviceNode);
            }
        } catch (SAXException | ParserConfigurationException sxe) {
            throw new IOException(sxe);
        }

        return services;
    }
}
