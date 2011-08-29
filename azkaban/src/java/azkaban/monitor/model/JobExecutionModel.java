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

import azkaban.app.LoggingJob;
import azkaban.common.jobs.Job;
import azkaban.jobcontrol.impl.jobs.ResourceThrottledJob;
import azkaban.jobcontrol.impl.jobs.RetryingJob;
import azkaban.monitor.MonitorInterface.JobState;

/**
 * Model class that encapsulates a running job instance, and variables there of
 * that would help in global statistics.
 *
 */
public class JobExecutionModel {
    
    /**
     * Name of the job.
     */
    protected String jobName;
    
    /** 
     * Ref. to workflow model of which this job is a part.
     */
    protected WorkflowExecutionModel workflow;
    
    /**
     * Start time for the job.
     */
    protected long startTime;
    
    /**
     * End time for the job.
     */
    protected long endTime;
    
    /**
     * Total execution (run) time for this job, to millisec.
     */
    protected long executionTime;     
    
    /**
     * Final job state.
     */
    protected JobState finalJobState;
    
    /**
     * Indicator that job has completed.
     */
    protected boolean completed;
    
    /**
     * Constructor.
     * @param jobName
     *            name of the job
     * @param workflow 
     *            workflow execution model to which job belongs
     */
    public JobExecutionModel(String jobName, WorkflowExecutionModel workflow) {
        this.workflow = workflow;
        this.jobName = jobName;
    }
    
    /**
     * Set the start time for the job execution, and contact eModel to update stats.
     * @param time
     *            epoch time in ms for the start for the job.
     */
    public void setStartTime(long time) {
        startTime = time;
    }
    
    /**
     * Set the end time for the job execution, and contact eModel to update stats.
     * @param time 
     *            epoch time in ms for the ending of the job.
     * @param jobState
     *            state of the job when it ended.
     */
    public void setEndTime(long time, JobState jobState) {
        endTime = time;
        executionTime = endTime - startTime;
        finalJobState = jobState;
        completed = true;
    }
    
    /**
     * getter
     * @return the job name
     */
    public String getJobName() {
        return jobName;
    }
    
    /**
     * getter
     * @return the execution time in ms.
     */
    public long getExecutionTime() {
        return executionTime;
    }
    
    /**
     * getter
     * @return the job state of the job when it ended.
     */
    public JobState getCompletionState() {
        return finalJobState;
    }
    
    /**
     * getter
     * @return the start time in epoch ms.
     */
    public long getStartTime() {
        return startTime;
    }
    
    /**
     * getter
     * @return the end time in epoch ms.
     */
    public long getEndTime() {
        return endTime;
    }
    
    /**
     * getter
     * @return the final job state.
     */
    public JobState getFinalJobState() {
        return finalJobState;
    }
    
    /**
     * getter
     * @return true if completed.
     */
    public boolean isCompleted() {
        return completed;
    }
    
    /**
     * getter
     * @return the workflow execution for this job execution.
     */
    public WorkflowExecutionModel getWorkflowModel() {
        return workflow;
    }
    
    /**
     * Public method to create the right kind of JobModel based on the given Job type.
     * @param job
     *            the Job whose model is being created.
     * @param workflow 
     *            the workflow execution for which the job model is created.
     * @return the JobExecutionModel
     */
    public static JobExecutionModel createJobModel(Job job, WorkflowExecutionModel workflow) {
        if (job instanceof RetryingJob) {
            return new RetryingJobModel(job.getId(), workflow);
        } else if (job instanceof ResourceThrottledJob) {
            return new ResourceThrottledJobModel(job.getId(), workflow);
        } else if (job instanceof LoggingJob) {
            return new LoggingJobModel(job.getId(), workflow);
        } else {
            return new JobExecutionModel(job.getId(), workflow);
        }
    }

}
