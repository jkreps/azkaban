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

import java.util.List;
import java.util.Map;

import azkaban.common.jobs.Job;
import azkaban.jobcontrol.impl.jobs.ResourceThrottledJob;
import azkaban.monitor.MonitorInterface.JobState;
import azkaban.monitor.MonitorInterface.WorkflowState;
import azkaban.monitor.stats.NativeGlobalStats;
import azkaban.monitor.stats.NativeJobClassStats;
import azkaban.monitor.stats.NativeWorkflowClassStats;

/**
 * Interface to ExecutionModel which encapsulates managing the workflow
 * and job model, as well as global statistics objects and notification
 * management.
 */
public interface ExecutionModel {
    
    /**
     * Tell the workflow model object that the workflow has been scheduled.
     * @param rootJobName
     *            root job name of this workflow.
     * @param time
     *            epoch time in ms for scheduling the workflow.
     */
    public void scheduleWorkflow(String rootJobName, long time);
    
    /**
     * Tell the workflow model object that the workflow has started.
     * @param rootJobName
     *            root job name of this workflow.
     * @param wfId
     *            the string version of int workflow id.
     * @param time 
     *            epoch time in ms for starting the workflow.
     */
    public void startWorkflow(String rootJobName, String wfId, long time);
    
    /**
     * Tell the workflow model object that the workflow has ended.
     * @param wfId
     *            the string version of int workflow id.
     * @param time 
     *            epoch time in ms for ending the workflow.
     * @param wfState
     *            the state of the workflow when it ended.
     */
    public void endWorkflow(String wfId, long time, WorkflowState wfState);
    
    /**
     * Tell the model object that a sub-job of it has started.
     * @param time 
     *            epoch time in ms for when this job started.
     * @param job
     *            the Job that is being started.
     */
    public void startWorkflowJob(long time, Job job);
    
    /**
     * Tell the model object that a sub-job has ended.
     * @param time 
     *            epoch time in ms when this job ended.
     * @param job
     *            the Job that is ended.
     * @param jobState
     *            the state of the job when it ended.
     */
    public void endWorkflowJob(long time, 
                               Job job, 
                               JobState jobState);
    
    /**
     * Set a resource wait time for a job in a workflow.
     * @param lockWaitTime
     *            time in ms logged for waiting on resources.
     * @param job 
     *            the ResourceThrottledJob
     */
    public void resourceWaitTime(long lockWaitTime, ResourceThrottledJob job);
    
    /**
     * Update global/class stats based on the workflow being scheduled.
     * @param wfModel
     *            the workflow execution model being updated for scheduled.
     */
    public void updateWorkflowScheduledStats(WorkflowExecutionModel wfModel);
    
    /**
     * Update global/class stats based on the workflow being started.
     * @param wfModel
     *            the workflow execution model being updated for started.
     */
    public void updateWorkflowStartedStats(WorkflowExecutionModel wfModel);
    
    /**
     * Update global/class stats based on the workflow being ended.
     * @param wfModel
     *            the workflow execution model being updated for ending.
     */
    public void updateWorkflowEndedStats(WorkflowExecutionModel wfModel);
    
    /**
     * Update the global/class stats based on the workflow job being started.
     * @param jobModel
     *            the job execution model being updated for started.
     */
    public void updateJobStartedStats(JobExecutionModel jobModel);
    
    /**
     * Update the global/class stats based on the workflow being completed.
     * @param jobModel
     *            the job execution model being jpdated for ending.
     */
    public void updateJobCompletedStats(JobExecutionModel jobModel);
    
    /**
     * Update/notify based on wait time for resource throttled job.
     * @param jobModel - ResourceThrottledJobModel
     */
    public void updateResourceThrottledJobLockWaitTime(ResourceThrottledJobModel jobModel);
    
    /**
     * Return a copy of the global statistics object.
     * @return a copy of the global statistics.
     */ 
    public NativeGlobalStats getGlobalStatsCopy();
    
    /**
     * Return a copy of all the known workflow class's being (or were) monitored.
     * @return the list of root job names for the workflow class stats.
     */
    public List<String> getWorkflowClassIds();
    
    /**
     * Get a copy of the workflow class statistics for some id.
     * @param wfRootJobName
     *            the root job name behind the workflow whose stats are requested.
     * @return the NativeWorkflowClassStats for the root job name provided.
     * 
     */
    public NativeWorkflowClassStats getWorkflowClassStatsById(String wfRootJobName);
    
    /**
     * Get a copy of all the workflow class statistics.
     * @return  Map<String, NativeWorkflowClassStats> mapping root job name to 
     *          the workflow class stats (copy)
     */
    public Map<String, NativeWorkflowClassStats> getAllWorkflowClassStats();
    
    /**
     * Get a list of all the job class's being (or were) monitored.
     * @return List<String> a list list of job class names whose global stats are
     *         being collected.
     */
    public List<String> getJobClassIds();
    
    /**
     * Get a copy of a job class statistics object
     * @param jobClassId
     *            class name whose statistics are being collected.
     * @return the NativeJobClassStats corresponding to the job class name provided.
     */
    public NativeJobClassStats getJobClassStatsById(String jobClassId);
    
    /**
     * Return a copy of all the job class statistics.
     * @return Map<String, NativeJobClassStats>
     */
    public Map<String, NativeJobClassStats> getAllJobClassStats();
    
    /**
     * Return the number of workflows executions, active and completed.
     * @return the number of workflow executions.
     */
    public long getNumberOfWorkflows();
    
    /**
     * Return the number of completed workflow executions.
     * @return the number completed workflow executions.
     */
    public long getNumberOfCompletedWorkflows();
    
    /**
     * Return a list of all currently completed workflow models.
     * @param reverseTime
     *            true indicates return list in reverse time order.
     * @return List<WorkflowExecutionModel>, a list of workflow execution models
     *         that have completed.
     */
    public List<WorkflowExecutionModel> getCompletedWorkflowModels(boolean reverseTime);
    
    /**
     * Return a list of workflow models completed prior to a given time.
     * @param endedBeforeTime
     *           Epoch time in ms prior to which workflows models should end.
     * @param reverseTime
     *           true indicagtes return list in reverse time order.
     * @return List<WorkflowExecutionModel>, a list of workflow execution models
     *         that have completed.
     */
    public List<WorkflowExecutionModel> getCompletedWorkflowModels(
                                             long endedBeforeTime, 
                                             boolean reverseTime);
    
    /**
     * Clear out all the completed workflow models - used to free storage.
     */
    public void clearCompletedWorkflows();
    
    /**
     * Clear out all workflow models completed before a given time from now.
     * @param endedBeforeTime
     *          Epoch time in ms before which completed workflows are deleted.
     */
    public void clearCompletedWorkflows(long endedBeforeTime);
}
