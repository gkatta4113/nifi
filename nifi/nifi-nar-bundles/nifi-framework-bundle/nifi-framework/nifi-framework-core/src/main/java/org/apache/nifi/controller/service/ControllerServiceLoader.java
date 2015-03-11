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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.nifi.controller.FlowFromDOMFactory;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.events.BulletinFactory;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.util.DomUtils;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

/**
 *
 */
public class ControllerServiceLoader {

    private static final Log logger = LogFactory.getLog(ControllerServiceLoader.class);


    public static List<ControllerServiceNode> loadControllerServices(final ControllerServiceProvider provider, final InputStream serializedStream, final StringEncryptor encryptor, final BulletinRepository bulletinRepo, final boolean autoResumeState) throws IOException {
        final DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
        documentBuilderFactory.setNamespaceAware(true);

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
            
            final Document document = builder.parse(in);
            final Element controllerServices = DomUtils.getChild(document.getDocumentElement(), "controllerServices");
            final List<Element> serviceElements = DomUtils.getChildElementsByTagName(controllerServices, "controllerService");
            return new ArrayList<ControllerServiceNode>(loadControllerServices(serviceElements, provider, encryptor, bulletinRepo, autoResumeState));
        } catch (SAXException | ParserConfigurationException sxe) {
            throw new IOException(sxe);
        }
    }
    
    public static Collection<ControllerServiceNode> loadControllerServices(final List<Element> serviceElements, final ControllerServiceProvider provider, final StringEncryptor encryptor, final BulletinRepository bulletinRepo, final boolean autoResumeState) {
        final Map<Element, ControllerServiceNode> nodeMap = new HashMap<>();
        for ( final Element serviceElement : serviceElements ) {
            final ControllerServiceNode serviceNode = createControllerService(provider, serviceElement, encryptor);
            nodeMap.put(serviceElement, serviceNode);
        }
        for ( final Map.Entry<Element, ControllerServiceNode> entry : nodeMap.entrySet() ) {
            configureControllerService(entry.getValue(), entry.getKey(), encryptor);
        }
        
        // Start services
        if ( autoResumeState ) {
            for ( final Map.Entry<Element, ControllerServiceNode> entry : nodeMap.entrySet() ) {
                final Element controllerServiceElement = entry.getKey();
                final ControllerServiceNode serviceNode = entry.getValue();
                
                final ControllerServiceDTO dto = FlowFromDOMFactory.getControllerService(controllerServiceElement, encryptor);
                final ControllerServiceState state = ControllerServiceState.valueOf(dto.getState());
                final boolean enable = (state == ControllerServiceState.ENABLED || state == ControllerServiceState.ENABLING);
                if (enable) {
                    try {
                        provider.enableReferencingServices(serviceNode);
                    } catch (final Exception e) {
                        logger.error("Failed to enable " + serviceNode + " due to " + e);
                        if ( logger.isDebugEnabled() ) {
                            logger.error("", e);
                        }
                        
                        bulletinRepo.addBulletin(BulletinFactory.createBulletin(
                                "Controller Service", Severity.ERROR.name(), "Could not start services referencing " + serviceNode + " due to " + e));
                        continue;
                    }
                    
                    try {
                        provider.enableControllerService(serviceNode);
                    } catch (final Exception e) {
                        logger.error("Failed to enable " + serviceNode + " due to " + e);
                        if ( logger.isDebugEnabled() ) {
                            logger.error("", e);
                        }
                        
                        bulletinRepo.addBulletin(BulletinFactory.createBulletin(
                                "Controller Service", Severity.ERROR.name(), "Could not start " + serviceNode + " due to " + e));
                    }
                }
            }
        }
        
        return nodeMap.values();
    }
    
    
    private static ControllerServiceNode createControllerService(final ControllerServiceProvider provider, final Element controllerServiceElement, final StringEncryptor encryptor) {
        final ControllerServiceDTO dto = FlowFromDOMFactory.getControllerService(controllerServiceElement, encryptor);
        
        final ControllerServiceNode node = provider.createControllerService(dto.getType(), dto.getId(), false);
        node.setName(dto.getName());
        node.setComments(dto.getComments());
        return node;
    }
    
    private static void configureControllerService(final ControllerServiceNode node, final Element controllerServiceElement, final StringEncryptor encryptor) {
        final ControllerServiceDTO dto = FlowFromDOMFactory.getControllerService(controllerServiceElement, encryptor);
        node.setAnnotationData(dto.getAnnotationData());
        
        for (final Map.Entry<String, String> entry : dto.getProperties().entrySet()) {
            if (entry.getValue() == null) {
                node.removeProperty(entry.getKey());
            } else {
                node.setProperty(entry.getKey(), entry.getValue());
            }
        }
    }
}
