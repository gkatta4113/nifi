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

import static java.util.Objects.requireNonNull;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.nifi.annotation.lifecycle.OnAdded;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.annotation.lifecycle.OnRemoved;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ConfiguredComponent;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.ValidationContextFactory;
import org.apache.nifi.controller.exception.ControllerServiceNotFoundException;
import org.apache.nifi.controller.exception.ProcessorLifeCycleException;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.processor.StandardValidationContextFactory;
import org.apache.nifi.util.ObjectHolder;
import org.apache.nifi.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class StandardControllerServiceProvider implements ControllerServiceProvider {

    private static final Logger logger = LoggerFactory.getLogger(StandardControllerServiceProvider.class);

    private final ProcessScheduler processScheduler;
    private final ConcurrentMap<String, ControllerServiceNode> controllerServices;
    private static final Set<Method> validDisabledMethods;

    static {
        // methods that are okay to be called when the service is disabled.
        final Set<Method> validMethods = new HashSet<>();
        for (final Method method : ControllerService.class.getMethods()) {
            validMethods.add(method);
        }
        for (final Method method : Object.class.getMethods()) {
            validMethods.add(method);
        }
        validDisabledMethods = Collections.unmodifiableSet(validMethods);
    }

    public StandardControllerServiceProvider(final ProcessScheduler scheduler) {
        // the following 2 maps must be updated atomically, but we do not lock around them because they are modified
        // only in the createControllerService method, and both are modified before the method returns
        this.controllerServices = new ConcurrentHashMap<>();
        this.processScheduler = scheduler;
    }

    private Class<?>[] getInterfaces(final Class<?> cls) {
        final List<Class<?>> allIfcs = new ArrayList<>();
        populateInterfaces(cls, allIfcs);
        return allIfcs.toArray(new Class<?>[allIfcs.size()]);
    }

    private void populateInterfaces(final Class<?> cls, final List<Class<?>> interfacesDefinedThusFar) {
        final Class<?>[] ifc = cls.getInterfaces();
        if (ifc != null && ifc.length > 0) {
            for (final Class<?> i : ifc) {
                interfacesDefinedThusFar.add(i);
            }
        }

        final Class<?> superClass = cls.getSuperclass();
        if (superClass != null) {
            populateInterfaces(superClass, interfacesDefinedThusFar);
        }
    }
    
    @Override
    public ControllerServiceNode createControllerService(final String type, final String id, final boolean firstTimeAdded) {
        if (type == null || id == null) {
            throw new NullPointerException();
        }
        
        final ClassLoader currentContextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            final ClassLoader cl = ExtensionManager.getClassLoader(type);
            final Class<?> rawClass;
            if ( cl == null ) {
                rawClass = Class.forName(type);
            } else {
                Thread.currentThread().setContextClassLoader(cl);
                rawClass = Class.forName(type, false, cl);
            }
            
            final Class<? extends ControllerService> controllerServiceClass = rawClass.asSubclass(ControllerService.class);

            final ControllerService originalService = controllerServiceClass.newInstance();
            final ObjectHolder<ControllerServiceNode> serviceNodeHolder = new ObjectHolder<>(null);
            final InvocationHandler invocationHandler = new InvocationHandler() {
                @Override
                public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
                    final ControllerServiceNode node = serviceNodeHolder.get();
                    if (node.isDisabled() && !validDisabledMethods.contains(method)) {
                        // Use nar class loader here because we are implicitly calling toString() on the original implementation.
                        try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                            throw new IllegalStateException("Cannot invoke method " + method + " on Controller Service " + originalService + " because the Controller Service is disabled");
                        } catch (final Throwable e) {
                            throw new IllegalStateException("Cannot invoke method " + method + " on Controller Service with identifier " + id + " because the Controller Service is disabled");
                        }
                    }

                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return method.invoke(originalService, args);
                    } catch (final InvocationTargetException e) {
                        // If the ControllerService throws an Exception, it'll be wrapped in an InvocationTargetException. We want
                        // to instead re-throw what the ControllerService threw, so we pull it out of the InvocationTargetException.
                        throw e.getCause();
                    }
                }
            };

            final ControllerService proxiedService;
            if ( cl == null ) {
                proxiedService = (ControllerService) Proxy.newProxyInstance(getClass().getClassLoader(), getInterfaces(controllerServiceClass), invocationHandler);
            } else {
                proxiedService = (ControllerService) Proxy.newProxyInstance(cl, getInterfaces(controllerServiceClass), invocationHandler);
            }
            logger.info("Create Controller Service of type {} with identifier {}", type, id);

            originalService.initialize(new StandardControllerServiceInitializationContext(id, this));

            final ValidationContextFactory validationContextFactory = new StandardValidationContextFactory(this);

            final ControllerServiceNode serviceNode = new StandardControllerServiceNode(proxiedService, originalService, id, validationContextFactory, this);
            serviceNodeHolder.set(serviceNode);
            serviceNode.setName(rawClass.getSimpleName());
            
            if ( firstTimeAdded ) {
                try (final NarCloseable x = NarCloseable.withNarLoader()) {
                    ReflectionUtils.invokeMethodsWithAnnotation(OnAdded.class, originalService);
                } catch (final Exception e) {
                    throw new ProcessorLifeCycleException("Failed to invoke On-Added Lifecycle methods of " + originalService, e);
                }
            }

            this.controllerServices.put(id, serviceNode);
            return serviceNode;
        } catch (final Throwable t) {
            throw new ControllerServiceNotFoundException(t);
        } finally {
            if (currentContextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(currentContextClassLoader);
            }
        }
    }
    
    @Override
    public void enableControllerService(final ControllerServiceNode serviceNode) {
        serviceNode.verifyCanEnable();
        
        try (final NarCloseable x = NarCloseable.withNarLoader()) {
            final ConfigurationContext configContext = new StandardConfigurationContext(serviceNode, this);
            ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnEnabled.class, serviceNode.getControllerServiceImplementation(), configContext);
        }
        
        serviceNode.enable();
    }
    
    @Override
    public void disableControllerService(final ControllerServiceNode serviceNode) {
        serviceNode.verifyCanDisable();

        // We must set the service to disabled before we invoke the OnDisabled methods because the service node
        // can throw Exceptions if we attempt to disable the service while it's known to be in use.
        serviceNode.disable();
        
        try (final NarCloseable x = NarCloseable.withNarLoader()) {
            ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnDisabled.class, serviceNode.getControllerServiceImplementation());
        }
    }

    @Override
    public ControllerService getControllerService(final String serviceIdentifier) {
        final ControllerServiceNode node = controllerServices.get(serviceIdentifier);
        return (node == null) ? null : node.getProxiedControllerService();
    }

    @Override
    public boolean isControllerServiceEnabled(final ControllerService service) {
        return isControllerServiceEnabled(service.getIdentifier());
    }

    @Override
    public boolean isControllerServiceEnabled(final String serviceIdentifier) {
        final ControllerServiceNode node = controllerServices.get(serviceIdentifier);
        return (node == null) ? false : !node.isDisabled();
    }

    @Override
    public ControllerServiceNode getControllerServiceNode(final String serviceIdentifier) {
        return controllerServices.get(serviceIdentifier);
    }

    @Override
    public Set<String> getControllerServiceIdentifiers(final Class<? extends ControllerService> serviceType) {
        final Set<String> identifiers = new HashSet<>();
        for (final Map.Entry<String, ControllerServiceNode> entry : controllerServices.entrySet()) {
            if (requireNonNull(serviceType).isAssignableFrom(entry.getValue().getProxiedControllerService().getClass())) {
                identifiers.add(entry.getKey());
            }
        }

        return identifiers;
    }
    
    @Override
    public String getControllerServiceName(final String serviceIdentifier) {
    	final ControllerServiceNode node = getControllerServiceNode(serviceIdentifier);
    	return node == null ? null : node.getName();
    }
    
    public void removeControllerService(final ControllerServiceNode serviceNode) {
        final ControllerServiceNode existing = controllerServices.get(serviceNode.getIdentifier());
        if ( existing == null || existing != serviceNode ) {
            throw new IllegalStateException("Controller Service " + serviceNode + " does not exist in this Flow");
        }
        
        serviceNode.verifyCanDelete();
        
        try (final NarCloseable x = NarCloseable.withNarLoader()) {
            final ConfigurationContext configurationContext = new StandardConfigurationContext(serviceNode, this);
            ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnRemoved.class, serviceNode.getControllerServiceImplementation(), configurationContext);
        }
        
        for ( final Map.Entry<PropertyDescriptor, String> entry : serviceNode.getProperties().entrySet() ) {
            final PropertyDescriptor descriptor = entry.getKey();
            if (descriptor.getControllerServiceDefinition() != null ) {
                final String value = entry.getValue() == null ? descriptor.getDefaultValue() : entry.getValue();
                if ( value != null ) {
                    final ControllerServiceNode referencedNode = getControllerServiceNode(value);
                    if ( referencedNode != null ) {
                        referencedNode.removeReference(serviceNode);
                    }
                }
            }
        }
        
        controllerServices.remove(serviceNode.getIdentifier());
    }
    
    @Override
    public Set<ControllerServiceNode> getAllControllerServices() {
    	return new HashSet<>(controllerServices.values());
    }
    
    @Override
    public void deactivateReferencingComponents(final ControllerServiceNode serviceNode) {
        deactivateReferencingComponents(serviceNode, new HashSet<ControllerServiceNode>());
    }
    
    private void deactivateReferencingComponents(final ControllerServiceNode serviceNode, final Set<ControllerServiceNode> visited) {
        final ControllerServiceReference reference = serviceNode.getReferences();
        
        final Set<ConfiguredComponent> components = reference.getActiveReferences();
        for (final ConfiguredComponent component : components) {
            if ( component instanceof ControllerServiceNode ) {
                // If we've already visited this component (there is a loop such that
                // we are disabling Controller Service A, but B depends on A and A depends on B)
                // we don't need to disable this component because it will be disabled after we return
                if ( visited.contains(component) ) {
                    continue;
                }
                
                visited.add(serviceNode);
                deactivateReferencingComponents((ControllerServiceNode) component, visited);
                
                if (isControllerServiceEnabled(serviceNode.getIdentifier())) {
                    serviceNode.verifyCanDisable(visited);
                    serviceNode.disable(visited);
                }
            } else if ( component instanceof ReportingTaskNode ) {
                final ReportingTaskNode taskNode = (ReportingTaskNode) component;
                if (taskNode.isRunning()) {
                    taskNode.verifyCanStop();
                    processScheduler.unschedule(taskNode);
                }
            } else if ( component instanceof ProcessorNode ) {
                final ProcessorNode procNode = (ProcessorNode) component;
                if ( procNode.isRunning() ) {
                    procNode.getProcessGroup().stopProcessor(procNode);
                }
            }
        }
    }
    
    
    @Override
    public void activateReferencingComponents(final ControllerServiceNode serviceNode) {
        activateReferencingComponents(serviceNode, new HashSet<ControllerServiceNode>());
    }
    
    
    /**
     * Recursively enables this controller service and any controller service that it references.
     * @param serviceNode
     */
    private void activateReferencedComponents(final ControllerServiceNode serviceNode) {
        for ( final Map.Entry<PropertyDescriptor, String> entry : serviceNode.getProperties().entrySet() ) {
            final PropertyDescriptor key = entry.getKey();
            if ( key.getControllerServiceDefinition() == null ) {
                continue;
            }
                
            final String serviceId = entry.getValue() == null ? key.getDefaultValue() : entry.getValue();
            if ( serviceId == null ) {
                continue;
            }
            
            final ControllerServiceNode referencedNode = getControllerServiceNode(serviceId);
            if ( referencedNode == null ) {
                throw new IllegalStateException("Cannot activate referenced component of " + serviceNode + " because no service exists with ID " + serviceId);
            }
            
            activateReferencedComponents(referencedNode);
            
            if ( referencedNode.isDisabled() ) {
                enableControllerService(referencedNode);
            }
        }
    }
    
    private void activateReferencingComponents(final ControllerServiceNode serviceNode, final Set<ControllerServiceNode> visited) {
        if ( serviceNode.isDisabled() ) {
            throw new IllegalStateException("Cannot activate referencing components of " + serviceNode.getControllerServiceImplementation() + " because the Controller Service is disabled");
        }
        
        final ControllerServiceReference ref = serviceNode.getReferences();
        final Set<ConfiguredComponent> components = ref.getReferencingComponents();
        
        // First, activate any other controller services. We do this first so that we can
        // avoid the situation where Processor X depends on Controller Services Y and Z; and
        // Controller Service Y depends on Controller Service Z. In this case, if we first attempted
        // to start Processor X, we would fail because Controller Service Y is disabled. THis way, we
        // can recursively enable everything.
        for ( final ConfiguredComponent component : components ) {
            if (component instanceof ControllerServiceNode) {
                final ControllerServiceNode componentNode = (ControllerServiceNode) component;
                activateReferencedComponents(componentNode);
                
                if ( componentNode.isDisabled() ) {
                    enableControllerService(componentNode);
                }
                
                activateReferencingComponents(componentNode);
            }
        }
        
        for ( final ConfiguredComponent component : components ) {
            if (component instanceof ProcessorNode) {
                final ProcessorNode procNode = (ProcessorNode) component;
                if ( !procNode.isRunning() ) {
                    procNode.getProcessGroup().startProcessor(procNode);
                }
            } else if (component instanceof ReportingTaskNode) {
                final ReportingTaskNode taskNode = (ReportingTaskNode) component;
                if ( !taskNode.isRunning() ) {
                    processScheduler.schedule(taskNode);
                }
            }
        }
    }
}
