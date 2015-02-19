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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.UUID;

import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.StandardProcessorNode;
import org.apache.nifi.controller.service.mock.DummyProcessor;
import org.apache.nifi.controller.service.mock.ServiceA;
import org.apache.nifi.controller.service.mock.ServiceB;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.processor.StandardProcessorInitializationContext;
import org.apache.nifi.processor.StandardValidationContextFactory;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestStandardControllerServiceProvider {

    @Test
    public void testDisableControllerService() {
        final ProcessScheduler scheduler = Mockito.mock(ProcessScheduler.class);
        final StandardControllerServiceProvider provider = new StandardControllerServiceProvider(scheduler);
        
        final ControllerServiceNode serviceNode = provider.createControllerService(ServiceB.class.getName(), "B", false);
        provider.enableControllerService(serviceNode);
        provider.disableControllerService(serviceNode);
    }
    
    @Test
    public void testEnableDisableWithReference() {
        final ProcessScheduler scheduler = Mockito.mock(ProcessScheduler.class);
        final StandardControllerServiceProvider provider = new StandardControllerServiceProvider(scheduler);
        
        final ControllerServiceNode serviceNodeB = provider.createControllerService(ServiceB.class.getName(), "B", false);
        final ControllerServiceNode serviceNodeA = provider.createControllerService(ServiceA.class.getName(), "A", false);
        
        serviceNodeA.setProperty(ServiceA.OTHER_SERVICE.getName(), "B");
        
        try {
            provider.enableControllerService(serviceNodeA);
            Assert.fail("Was able to enable Service A but Service B is disabled.");
        } catch (final IllegalStateException expected) {
        }
        
        provider.enableControllerService(serviceNodeB);
        provider.enableControllerService(serviceNodeA);
        
        try {
            provider.disableControllerService(serviceNodeB);
            Assert.fail("Was able to disable Service B but Service A is enabled and references B");
        } catch (final IllegalStateException expected) {
        }
        
        provider.disableControllerService(serviceNodeA);
        provider.disableControllerService(serviceNodeB);
    }
    
    
    @Test
    public void testEnableReferencingServicesGraph() {
        final ProcessScheduler scheduler = Mockito.mock(ProcessScheduler.class);
        final StandardControllerServiceProvider provider = new StandardControllerServiceProvider(scheduler);
        
        // build a graph of controller services with dependencies as such:
        //
        // A -> B -> D
        // C ---^----^
        //
        // In other words, A references B, which references D.
        // AND
        // C references B and D.
        //
        // So we have to verify that if D is enabled, when we enable its referencing services,
        // we enable C and B, even if we attempt to enable C before B... i.e., if we try to enable C, we cannot do so
        // until B is first enabled so ensure that we enable B first.
        
        final ControllerServiceNode serviceNode1 = provider.createControllerService(ServiceA.class.getName(), "1", false);
        final ControllerServiceNode serviceNode2 = provider.createControllerService(ServiceA.class.getName(), "2", false);
        final ControllerServiceNode serviceNode3 = provider.createControllerService(ServiceA.class.getName(), "3", false);
        final ControllerServiceNode serviceNode4 = provider.createControllerService(ServiceB.class.getName(), "4", false);
        
        serviceNode1.setProperty(ServiceA.OTHER_SERVICE.getName(), "2");
        serviceNode2.setProperty(ServiceA.OTHER_SERVICE.getName(), "4");
        serviceNode3.setProperty(ServiceA.OTHER_SERVICE.getName(), "2");
        serviceNode3.setProperty(ServiceA.OTHER_SERVICE_2.getName(), "4");
        
        provider.enableControllerService(serviceNode4);
        provider.enableReferencingServices(serviceNode4);
        
        assertEquals(ControllerServiceState.DISABLED, serviceNode3.getState());
        assertEquals(ControllerServiceState.DISABLED, serviceNode2.getState());
        assertEquals(ControllerServiceState.DISABLED, serviceNode1.getState());
    }
    
    
    @Test
    public void testStartStopReferencingComponents() {
        final ProcessScheduler scheduler = Mockito.mock(ProcessScheduler.class);
        final StandardControllerServiceProvider provider = new StandardControllerServiceProvider(scheduler);
        
        // build a graph of reporting tasks and controller services with dependencies as such:
        //
        // Processor P1 -> A -> B -> D
        // Processor P2 -> C ---^----^
        //
        // In other words, Processor P1 references Controller Service A, which references B, which references D.
        // AND
        // Processor P2 references Controller Service C, which references B and D.
        //
        // So we have to verify that if D is enabled, when we enable its referencing services,
        // we enable C and B, even if we attempt to enable C before B... i.e., if we try to enable C, we cannot do so
        // until B is first enabled so ensure that we enable B first.
        
        final ControllerServiceNode serviceNode1 = provider.createControllerService(ServiceA.class.getName(), "1", false);
        final ControllerServiceNode serviceNode2 = provider.createControllerService(ServiceA.class.getName(), "2", false);
        final ControllerServiceNode serviceNode3 = provider.createControllerService(ServiceA.class.getName(), "3", false);
        final ControllerServiceNode serviceNode4 = provider.createControllerService(ServiceB.class.getName(), "4", false);
        
        final ProcessGroup mockProcessGroup = Mockito.mock(ProcessGroup.class);
        Mockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                final ProcessorNode procNode = (ProcessorNode) invocation.getArguments()[0];
                procNode.verifyCanStart();
                procNode.setScheduledState(ScheduledState.RUNNING);
                return null;
            }
        }).when(mockProcessGroup).startProcessor(Mockito.any(ProcessorNode.class));
        
        Mockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(final InvocationOnMock invocation) throws Throwable {
                final ProcessorNode procNode = (ProcessorNode) invocation.getArguments()[0];
                procNode.verifyCanStop();
                procNode.setScheduledState(ScheduledState.STOPPED);
                return null;
            }
        }).when(mockProcessGroup).stopProcessor(Mockito.any(ProcessorNode.class));
        
        final String id1 = UUID.randomUUID().toString();
        final ProcessorNode procNodeA = new StandardProcessorNode(new DummyProcessor(), id1,
                new StandardValidationContextFactory(provider), scheduler, provider);
        procNodeA.getProcessor().initialize(new StandardProcessorInitializationContext(id1, null, provider));
        procNodeA.setProperty(DummyProcessor.SERVICE.getName(), "1");
        procNodeA.setProcessGroup(mockProcessGroup);
        
        final String id2 = UUID.randomUUID().toString();
        final ProcessorNode procNodeB = new StandardProcessorNode(new DummyProcessor(),id2,
                new StandardValidationContextFactory(provider), scheduler, provider);
        procNodeB.getProcessor().initialize(new StandardProcessorInitializationContext(id2, null, provider));
        procNodeB.setProperty(DummyProcessor.SERVICE.getName(), "3");
        procNodeB.setProcessGroup(mockProcessGroup);
        
        serviceNode1.setProperty(ServiceA.OTHER_SERVICE.getName(), "2");
        serviceNode2.setProperty(ServiceA.OTHER_SERVICE.getName(), "4");
        serviceNode3.setProperty(ServiceA.OTHER_SERVICE.getName(), "2");
        serviceNode3.setProperty(ServiceA.OTHER_SERVICE_2.getName(), "4");
        
        provider.enableControllerService(serviceNode4);
        provider.enableReferencingServices(serviceNode4);
        provider.scheduleReferencingComponents(serviceNode4);
        
        assertEquals(ControllerServiceState.DISABLED, serviceNode3.getState());
        assertEquals(ControllerServiceState.DISABLED, serviceNode2.getState());
        assertEquals(ControllerServiceState.DISABLED, serviceNode1.getState());
        assertTrue(procNodeA.isRunning());
        assertTrue(procNodeB.isRunning());
        
        // stop processors and verify results.
        provider.unscheduleReferencingComponents(serviceNode4);
        assertFalse(procNodeA.isRunning());
        assertFalse(procNodeB.isRunning());
        assertEquals(ControllerServiceState.DISABLED, serviceNode3.getState());
        assertEquals(ControllerServiceState.DISABLED, serviceNode2.getState());
        assertEquals(ControllerServiceState.DISABLED, serviceNode1.getState());
        
        provider.disableReferencingServices(serviceNode4);
        assertEquals(ControllerServiceState.DISABLED, serviceNode3.getState());
        assertEquals(ControllerServiceState.DISABLED, serviceNode2.getState());
        assertEquals(ControllerServiceState.DISABLED, serviceNode1.getState());
        assertEquals(ControllerServiceState.ENABLED, serviceNode4.getState());
        
        provider.disableControllerService(serviceNode4);
        assertEquals(ControllerServiceState.DISABLED, serviceNode4.getState());
    }
}
