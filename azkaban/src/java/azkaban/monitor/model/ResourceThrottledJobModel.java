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
 * Job model for resource throttled jobs.
 *
 */
public class ResourceThrottledJobModel extends DelegatingJobModel {
   
    /**
     * Resource lock wait time in ms.
     */
    private long lockWaitTime;
   
    /**
     * Constructor.
     * @param jobName
     *            the name of the job behind this job model.
     * @param workflow 
     *            the workflow execution to which this job belongs.
     */
    public ResourceThrottledJobModel(String jobName, WorkflowExecutionModel workflow) {
        super(jobName, workflow);
    }
    
    /**
     * Set the waiting time for the locks, and inform eModel to update stats.
     * @param lockWaitTime
     *            the resource wait time in ms.
     */
    public void setResourceLockWaitTime(long lockWaitTime) {
        this.lockWaitTime = lockWaitTime;
    }
    
    /**
     * getter
     * @return the lock wait time.
     */
    public long getLockWaitTime() {
        return lockWaitTime;
    }
}
