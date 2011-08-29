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

import java.util.LinkedList;

/**
 * Job model for retrying jobs.  Each instance maintains a reverse list of executions, with
 * the first member being the most recent.
 *
 */
public class RetryingJobModel extends DelegatingJobModel {
    
    /**
     * This is the list of executions, a stack, with the most current
     * to the front.
     */
    LinkedList<JobExecutionModel> executions = new LinkedList<JobExecutionModel>();
    
    /**
     * Constructor.
     * @param jobName
     *           the job name behind this retrying job.
     * @param workflow
     *           the workflow exection to which this job belongs.
     */
    public RetryingJobModel(String jobName, WorkflowExecutionModel workflow) {
        super(jobName, workflow);
    }
    
    /**
     * Note that in this case the set inner job model actually appends the model
     * to the stack of executions.
     */
    public synchronized void setInnerJobModel(JobExecutionModel jobModel) {
        super.setInnerJobModel(jobModel);
        // This is one of our new executions
        executions.addFirst(jobModel);      
    }
    
    /**
     * getter
     * @return the number of tries made on the inner job.
     */
    public synchronized long getNumberOfTries() {
        return executions.size();
    }
}
