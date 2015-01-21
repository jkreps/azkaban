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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import azkaban.common.jobs.DelegatingJob;
import azkaban.common.jobs.Job;
import azkaban.jobcontrol.impl.jobs.ResourceThrottledJob;
import azkaban.monitor.MonitorInterface.JobState;
import azkaban.monitor.MonitorInterface.WorkflowState;

/**
 * Model class that encapsulates a running workflow, and variables thereof that
 * would assist in determining global statistics.
 *
 */
public class WorkflowExecutionModel {
    private static final Logger logger = Logger.getLogger(WorkflowExecutionModel.class);
    
    /**
     * If the workflow is execution multiple time - with same id - 
     * this points to the last older version.
     */
    private WorkflowExecutionModel olderExecution;
    
    /**
     * The workflow id - string version of integer id value.
     */
    private String wfId;
    
    /**
     * Name of root job to workflow.
     */
    private String rootJobName;
    
    /**
     * Time workflow was scheduled.
     */
    private long scheduledTime;
    
    /**
     * Start time of workflow.
     */
    private long startTime;
    
    /**
     * End time of workflow.
     */
    private long endTime;
    
    /** 
     * Amount of time spent pending (in millisec.).
     */
    private long pendingTime;
    
    /**
     * Amount of time spent in execution (in millisec.).
     */
    private long executionTime;
    
    /**
     * Total elapsed time (scheduled to termination) in millisec.
     */
    private long elapsedTime;
    
    /**
     * Final state of workflow.
     */
    private WorkflowState finalWorkflowState = WorkflowState.UNKNOWN;
    
    /**
     * Indication of workflow completion.
     */
    private boolean completed;
    
    /**
     * Flow directory to managing job delegating map.
     */
    private FlowDirectory flowDirectory;
    
    /**
     * Map a Job to the job model used in this workflow execution.
     */
    private Map<Job, JobExecutionModel> jobModelMapper = new ConcurrentHashMap<Job, JobExecutionModel>();
    
    /**
     * List of job executions belonging to this workflow.
     */
    private List<JobExecutionModel> wfExecutions = new ArrayList<JobExecutionModel>();
    
    /**
     * Constructor.
     * @param wfId
     *            string variant of int workflow id assigned to workflow by engine.
     * @param rootJobName
     *            name of the root job behind this workflow.
     */
    public WorkflowExecutionModel(String wfId, String rootJobName) {
        this.wfId = wfId;
        this.rootJobName = rootJobName;
        
        flowDirectory = FlowDirectory.getFlowDirectory();
    }
    
    /**
     * Constructor, accounting for re-execution with same workflow id.
     * @param wfId 
     *            string variant of int workflow id assigned to workflow by engine.
     * @param rootJobName
                  name of the root job behind this workflow.
     * @param olderExecution 
     *            the prior execution this execution supersedes e.g. if workflow
     *            is restarted with same wfId.
     */
    public WorkflowExecutionModel(String wfId, 
                         String rootJobName, 
                         WorkflowExecutionModel olderExecution) {
        this(wfId, rootJobName);
        this.olderExecution = olderExecution;
    }
    
    /**
     * Set the scheduled time.
     * @param time 
     *            epoch time in ms workflow was scheduled.
     */
    public void setScheduledTime(long time) {
        scheduledTime = time;
    }
    
    /**
     * Set the start time.
     * @param time
     *            epoch time in ms workflow was started.
     */
    public void setStartTime(long time) {
        startTime = time;
        
        if (scheduledTime == 0) {
            setScheduledTime(startTime); // no explicit schedule.
        }
        
        pendingTime = startTime - scheduledTime;
    }
    
    /**
     * Set the end time for the full workflow.
     * @param time
     *            epoch time this workflow was ended.
     * @param wfState
     *            state of workflow execution when ended.
     */
    public void setEndTime(long time, WorkflowState wfState) {
        endTime = time;
        
        if (startTime == 0) {
            String errorMsg = "Workflow start time never set for wf id:" + wfId;
            logger.error(errorMsg);
            return;
        }
        
        executionTime = endTime - startTime;
        
        if (scheduledTime == 0) {
            String errorMsg = "Workflow scheduled time never set for wf id:" + wfId;
            logger.error(errorMsg);
            return;
        }
       
        elapsedTime = endTime - scheduledTime;
        
        finalWorkflowState = wfState;    
        
        // Flag completed after final update of stats.
        completed = true;
    }
   
    /**
     * Indicate that a job started in this workflow.
     * @param job
     *            Job that was started in this workflow.
     * @param time 
     *            epoch time in ms job was started
     * @return JobExecutionModel for started job.
     */
    public JobExecutionModel startJob(Job job, long time) {
        JobExecutionModel jobModel = jobModelMapper.get(job);
        if (jobModel == null) {
            jobModel = createJobModel(job);
        }
        jobModel.setStartTime(time);
        return jobModel;
    }
    
    /**
     * Indicate that a job ended in this workflow.
     * @param job
     *            Job has has ended.
     * @param time 
     *            epoch based time in ms when job ended.
     * @param jobState
     *            state of job when it ended.
     * @return JobExecutionModel for job.
     */
    public JobExecutionModel endJob(Job job, long time, JobState jobState) {
        JobExecutionModel jobModel = jobModelMapper.get(job);
        if (jobModel == null) {
            logger.debug("null job execution model");
            return null;
        }
        jobModel.setEndTime(time, jobState);
        jobModelMapper.remove(job);     // We are done with this job model.
        
        flowDirectory.removeJobReference(job);       
        return jobModel;
    }
    
    /**
     * Set the the resource wait time on the resource throttled job.
     * @param job
     *            ResourceThrottledJob whose lock time is being set.
     * @param lockWaitTime 
     *            time in ms for resource lock
     * @return ResourceThrottledJobModel for this job.
     */
    public ResourceThrottledJobModel resourceWaitTime(ResourceThrottledJob job, long lockWaitTime) {
        JobExecutionModel jobModel = jobModelMapper.get(job);
        if (jobModel == null) {
            logger.debug("null job execution model");
            return null;
        }
        if (!(jobModel instanceof ResourceThrottledJobModel)) {
            logger.debug("job execution model not ResourceThrottledModel");
            return null;
        }
        ((ResourceThrottledJobModel)jobModel).setResourceLockWaitTime(lockWaitTime);
        return (ResourceThrottledJobModel)jobModel;
    }
    
    /**
     * Create a job model based on the given job, with due consideration if it is
     * a delegating job - which needs the corresponding DelegatingJobModel.
     * @param job
     *            Job whose job execution model is being created.
     * @return JobExecutionModel for job.
     */
    private JobExecutionModel createJobModel(Job job) {
        JobExecutionModel jobModel = JobExecutionModel.createJobModel(job, this);
        
        DelegatingJob delegatingJob = flowDirectory.getDelegatingJobParent(job);
        if (delegatingJob != null) {
            // This means that the job is the inner job of a delegating job;
            DelegatingJobModel delegatingJobModel = (DelegatingJobModel)jobModelMapper.get(delegatingJob);
            // The following provides a kind of notification to the delegating parent to account
            // for its inner job accounting.
            delegatingJobModel.setInnerJobModel(jobModel);
        }
        jobModelMapper.put(job, jobModel);
        wfExecutions.add(jobModel);
        return jobModel;
    }
    
    /**
     * Get all the workflow's execution jobs by name.
     * @return a list of string names.
     */
    public List<String> getWorkflowJobNames() {
        List<String> jobnames = new ArrayList<String>();
        for (JobExecutionModel jobModel : wfExecutions) {
            jobnames.add(jobModel.getJobName());
        }
        return jobnames;
    }
    
    /**
     * Get all workflow's job executions.
     * @return a list of JobExecutionModels
     */
    public List<JobExecutionModel> getWorkflowJobExecutions() {
        List<JobExecutionModel> jobModels = new ArrayList<JobExecutionModel>();
        for (JobExecutionModel jobModel : wfExecutions) {
            jobModels.add(jobModel);
        }
        return jobModels;
    }
    
    /**
     * getter
     * @return workflow pending time in ms.
     */
    public long getPendingTime() {
        return pendingTime;
    }
    
    /**
     * getter
     * @return execution time in ms.
     */
    public long getExecutionTime() {
        return executionTime;
    }
    
    /**
     * getter
     * @return pending + execution time in ms.
     */
    public long getElapsedTime() {
        return elapsedTime;
    }
    
    /**
     * getter
     * @return final WorkflowState value.
     */
    public WorkflowState getCompletionState() {
        return finalWorkflowState;
    }
    
    /**
     * getter
     * @return the workflow id as string variant of int workflow id.
     */
    public String getWorkflowId() {
        return wfId;
    }
    
    /**
     * getter
     * @return the root job name of workflow.
     */
    public String getRootJobName() {
        return rootJobName;
    }
    
    /**
     * getter
     * @return true if workflow is completed.
     */
    public boolean isCompleted() {
        return completed;
    }
    
    /**
     * getter
     * @return time in epoch ms for start time.
     */
    public long getStartTime() {
        return startTime;
    }
    
    /**
     * getter
     * @return  time in epcoh ms for end time.
     */
    public long getEndTime() {
        return endTime;
    }
    
    /**
     * getter
     * @return return the final workflow state.
     */
    public WorkflowState getFinalWorkflowState() {
        return finalWorkflowState;
    }
    
    /**
     * getter
     * @return  get the next older execution model.
     */
    public WorkflowExecutionModel getOlderExecutionModel() {
        return olderExecution;
    }

    /**
     * sets the wfId
     * @param wfId to set
     */
    public void setWorkflowId(String wfId) {
        this.wfId = wfId;
    }

}
