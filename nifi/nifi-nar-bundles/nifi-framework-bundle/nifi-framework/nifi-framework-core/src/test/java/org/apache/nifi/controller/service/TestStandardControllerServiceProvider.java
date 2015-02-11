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

import static org.junit.Assert.assertFalse;

import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.service.mock.ServiceA;
import org.apache.nifi.controller.service.mock.ServiceB;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

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
    public void testActivateReferencingComponentsGraph() {
        final ProcessScheduler scheduler = Mockito.mock(ProcessScheduler.class);
        final StandardControllerServiceProvider provider = new StandardControllerServiceProvider(scheduler);
        
        // build a graph of components with dependencies as such:
        //
        // A -> B -> D
        // C ---^----^
        //
        // In other words, A references B, which references D.
        // AND
        // C references B and D.
        //
        // So we have to verify that if D is enabled, when we enable its referencing components,
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
        provider.activateReferencingComponents(serviceNode4);
        
        assertFalse(serviceNode3.isDisabled());
        assertFalse(serviceNode2.isDisabled());
        assertFalse(serviceNode1.isDisabled());
        
    }
    
}
