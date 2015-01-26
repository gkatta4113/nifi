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
package org.apache.nifi.web.api.dto;

import java.util.Map;

import javax.xml.bind.annotation.XmlType;

/**
 * Component that is capable of reporting internal NiFi state to an external service
 */
@XmlType(name = "reportingTask")
public class ReportingTaskDTO extends NiFiComponentDTO {
	private String name;
	private String comment;
	private String type;
	private String schedulingPeriod;
	private String scheduledState;
	private String schedulingStrategy;
	private String availability;
	
	private Map<String, String> properties;
    private Map<String, PropertyDescriptorDTO> descriptors;
 
    private String annotationData;

    /**
     * The user-defined name of the reporting task
     * @return
     */
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	/**
	 * The user-defined comment for the reporting task
	 * @return
	 */
	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	/**
	 * The type of reporting task
	 * @return
	 */
	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	/**
     * The frequency with which to schedule the reporting task. The format of the value will
     * depend on the value of {@link #getSchedulingStrategy()}.
     *
     * @return The scheduling period
     */
	public String getSchedulingPeriod() {
		return schedulingPeriod;
	}

	public void setSchedulingPeriod(String schedulingPeriod) {
		this.schedulingPeriod = schedulingPeriod;
	}

	/**
	 * The current scheduling state of the reporting task
	 * @return
	 */
	public String getScheduledState() {
		return scheduledState;
	}

	public void setScheduledState(String scheduledState) {
		this.scheduledState = scheduledState;
	}

	/**
	 * The scheduling strategy that determines how the {@link #getSchedulingPeriod()} value should
	 * be interpreted
	 * 
	 * @return
	 */
	public String getSchedulingStrategy() {
		return schedulingStrategy;
	}

	public void setSchedulingStrategy(String schedulingStrategy) {
		this.schedulingStrategy = schedulingStrategy;
	}

	/**
     * Where this service is available. Possible values are CLUSTER_MANAGER_ONLY, NODE_ONLY, BOTH.
     * 
     * @return 
     */
	public String getAvailability() {
		return availability;
	}

	public void setAvailability(String availability) {
		this.availability = availability;
	}

	/**
	 * The reporting task's properties
	 * @return
	 */
	public Map<String, String> getProperties() {
		return properties;
	}

	public void setProperties(Map<String, String> properties) {
		this.properties = properties;
	}

	/**
	 * Map of property name to descriptor
	 * @return
	 */
	public Map<String, PropertyDescriptorDTO> getDescriptors() {
		return descriptors;
	}

	public void setDescriptors(Map<String, PropertyDescriptorDTO> descriptors) {
		this.descriptors = descriptors;
	}

	/**
	 * The currently configured annotation data for the reporting task
	 * @return
	 */
	public String getAnnotationData() {
		return annotationData;
	}

	public void setAnnotationData(String annotationData) {
		this.annotationData = annotationData;
	}
	
}
