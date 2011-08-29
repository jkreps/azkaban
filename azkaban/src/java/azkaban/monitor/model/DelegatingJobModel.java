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
 * Class that builds on JobModel for Azkaban delegating jobs.  We are following along the line of
 * Job inheritance in Azkaban - so the job models inherit from this class, e.g. RetryingJobModel.
 *
 */
public class DelegatingJobModel extends JobExecutionModel {   
    
    /**
     * Identified the 'current' job model for the inner job.
     */
    private JobExecutionModel innerJobModel;
   
    /**
     * Constructor.
     * @param jobName
     *            the name of this job
     * @param workflow 
     *            the workflow execution model to which this job belongs.
     */
    public DelegatingJobModel(String jobName, WorkflowExecutionModel workflow) {
        super(jobName, workflow);
    }
    
    /**
     * setter
     * @param innerJobModel 
     *            the corresponding inner job model for this job model.
     */
    public void setInnerJobModel(JobExecutionModel innerJobModel) {
        this.innerJobModel = innerJobModel;
    }
    
    /**
     * getter
     * @return the inner job model for this delegating job model.
     */
    public JobExecutionModel getInnerJobModel() {
        return innerJobModel;
    }
}
