/*
 * Copyright 2010 Adconion, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */package azkaban.monitor.model;

/**
 * Corresponding model for Logging Job.
 *
 */
public class LoggingJobModel extends DelegatingJobModel {

    /**
     * Constructor.
     * @param jobName
     *            the name of the job which is being logged.
     * @param workflow 
     *            the workflow execution behind this job.
     */
    public LoggingJobModel(String jobName, WorkflowExecutionModel workflow) {
        super(jobName, workflow);
    }
}
