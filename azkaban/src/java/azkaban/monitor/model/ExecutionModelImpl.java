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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.joda.time.DateTimeUtils;

import azkaban.common.jobs.Job;
import azkaban.jobcontrol.impl.jobs.ResourceThrottledJob;
import azkaban.monitor.MonitorInternalInterface;
import azkaban.monitor.MonitorInterface.GlobalNotificationType;
import azkaban.monitor.MonitorInterface.JobState;
import azkaban.monitor.MonitorInterface.WorkflowState;
import azkaban.monitor.stats.GlobalStats;
import azkaban.monitor.stats.JobClassStats;
import azkaban.monitor.stats.NativeGlobalStats;
import azkaban.monitor.stats.NativeJobClassStats;
import azkaban.monitor.stats.NativeWorkflowClassStats;
import azkaban.monitor.stats.WorkflowClassStats;

/**
 * Class that holds all workflow and job models, and also holds and manages
 * the global statistics, including global stats, workflow class stats, and
 * job class stats.
 */
public class ExecutionModelImpl implements ExecutionModel {
    private static final Logger logger = Logger.getLogger(ExecutionModelImpl.class);
    
    /**
     * Reference to internal notification methods.
     */
    private MonitorInternalInterface monitor;
    
    /**
     * Map model name to WorkflowModel.
     */
    private Map<String, WorkflowExecutionModel> wfModels = new ConcurrentHashMap<String, WorkflowExecutionModel>();
    
    /**
     * Map from job name to WorkflowExecutionModel for all executions that are scheduled.
     */
    private Map<String, WorkflowExecutionModel> scheduledModels = new ConcurrentHashMap<String, WorkflowExecutionModel>();
    
    /**
     * Global Azkaban Statistics - only one instance necessary.
     */
    private GlobalStats globalStats;
    
    /**
     * FlowDirectory to manage job mappings to flowid and delegate
     */
    private FlowDirectory flowDirectory;
    
    /**
     * Map workflow name to Workflow Class Statistics.
     */
    private Map<String, WorkflowClassStats> workflowClassStats = 
        new ConcurrentHashMap<String, WorkflowClassStats> ();
    
    /**
     * Map Job name to Job Class Statistics.
     */
    private Map<String, JobClassStats> jobClassStats = 
        new ConcurrentHashMap<String, JobClassStats>();
    
    /**
     * Constructor.
     * @param monitor 
     *            MonitorInternalInterface
     */
    public ExecutionModelImpl(MonitorInternalInterface monitor) { 
        this.monitor = monitor;
        globalStats = new GlobalStats();
        
        flowDirectory = FlowDirectory.getFlowDirectory();
    }
      
    //*************** All Basic Update Activity               **************//

    public synchronized void scheduleWorkflow(String rootJobName, long time) {
        // Assume only one scheduled workflow at a time.  If one is already there, then
        // we overwrite it.
        WorkflowExecutionModel scheduledModel = new WorkflowExecutionModel(null, rootJobName);
        scheduledModel.setScheduledTime(time);
        scheduledModels.put(rootJobName, scheduledModel);
        updateWorkflowScheduledStats(scheduledModel);
    }
    
    public synchronized void startWorkflow(String rootJobName, String wfId, long time) {
        // If the workflow model is scheduled, then let's remove it and promote it to the
        // wfModels map.
        WorkflowExecutionModel wfModel = scheduledModels.get(rootJobName);
        if (wfModel == null) {
            // no schedule exists
            WorkflowExecutionModel existingWorkflow = wfModels.get(wfId);
            if (wfModels.get(wfId) != null) {
                // this is for retries.
                wfModel =  new WorkflowExecutionModel(wfId, rootJobName, existingWorkflow);
            } else {
                wfModel = new WorkflowExecutionModel(wfId, rootJobName);
            }
        }
        else {
            // job was scheduled, since it's starting remove it from scheduled.
            scheduledModels.remove(rootJobName);
            wfModel.setWorkflowId(wfId);
        }
        
        wfModels.put(wfId, wfModel);   
        wfModel.setStartTime(time);
        
        updateWorkflowStartedStats(wfModel);
    }
    
    public synchronized void endWorkflow(String wfId, long time, WorkflowState wfState) {
        WorkflowExecutionModel wfModel = wfModels.get(wfId);
        if (wfModel == null) {
            logger.error("No workflow model exists with id: " + wfId);
            return;
        }
        wfModel.setEndTime(time, wfState);
        
        updateWorkflowEndedStats(wfModel);
    }
    
    public synchronized void startWorkflowJob(long time, Job job) {
        String wfId = flowDirectory.getFlowId(job);
        if (wfId == null) {
            wfId = flowDirectory.mapJob(job);
            if (wfId == null) {
                logger.info("Could not find flow id for job: " + job.getClass().getSimpleName());
                return;
            }
        }
              
        WorkflowExecutionModel wfModel = wfModels.get(wfId);
        if (wfModel == null) {
            logger.error("No workflow model exists with id: " + wfId);
            return;
        }
        JobExecutionModel jobModel = wfModel.startJob(job, time);
        
        updateJobStartedStats(jobModel);
    }
    
    public synchronized void endWorkflowJob(long time, 
                                            Job job, 
                                            JobState jobState) {
        String wfId = flowDirectory.getFlowId(job);
        if (wfId == null) {
            logger.info("Could not find workflow id for job: " + job.getClass().getSimpleName());
            return;
        }
        
        WorkflowExecutionModel wfModel = wfModels.get(wfId);
        if (wfModel == null) {
            logger.error("No workflow model exists with id: " + wfId);
            return;
        }
        JobExecutionModel jobExecutionModel = wfModel.endJob(job, time, jobState);
        
        updateJobCompletedStats(jobExecutionModel);
        
        flowDirectory.removeJobReference(job);   
    }
    
    public synchronized void resourceWaitTime(long lockWaitTime, ResourceThrottledJob job) {
        String wfId = flowDirectory.getFlowId(job);
        if (wfId == null) {
            logger.info("Could not find workflow id for job: " + job.getClass().getSimpleName());
            return;
        }
        
        WorkflowExecutionModel wfModel = wfModels.get(wfId);
        if (wfModel == null) {
            logger.error("No workflow model exists with id: " + wfId);
            return;
        }
        ResourceThrottledJobModel jModel = wfModel.resourceWaitTime(job, lockWaitTime);
        
        updateResourceThrottledJobLockWaitTime(jModel);
    }
     
    //*************** Model object stats update methods       ***************
    
    public synchronized void updateWorkflowScheduledStats(WorkflowExecutionModel wfModel) {
        // Update the global stats based on this action
        globalStats.updateWorkflowScheduledStats(wfModel);
        
        // Update the workflow class stats based on this action
        WorkflowClassStats wfClassStats = getWorkflowClassStats(wfModel.getRootJobName());
        wfClassStats.updateWorkflowScheduledStats(wfModel);
        
        sendWorkflowNotifications(wfModel);
    }
    
    public synchronized void updateWorkflowStartedStats(WorkflowExecutionModel wfModel) {
        // Update the global stats based on this action
        globalStats.updateWorkflowStartedStats(wfModel);
        
        // Update the workflow class stats based on this action
        WorkflowClassStats wfClassStats = getWorkflowClassStats(wfModel.getRootJobName());
        wfClassStats.updateWorkflowStartedStats(wfModel);
        
        sendWorkflowNotifications(wfModel);
    }
    
    public synchronized void updateWorkflowEndedStats(WorkflowExecutionModel wfModel) {
        globalStats.updateWorkflowEndedStats(wfModel);
        
        WorkflowClassStats wfClassStats = getWorkflowClassStats(wfModel.getRootJobName());
        wfClassStats.updateWorkflowEndedStats(wfModel);
       
        sendWorkflowNotifications(wfModel);
    }
    
    private synchronized void sendWorkflowNotifications(WorkflowExecutionModel wfModel) {
        monitor.executeGlobalNotify(GlobalNotificationType.GLOBAL_STATS_CHANGE,
                new NativeGlobalStats(globalStats));
        
        WorkflowClassStats wfClassStats = getWorkflowClassStats(wfModel.getRootJobName());
        monitor.executeWorkflowClassNotify(new NativeWorkflowClassStats(
                wfClassStats));
        monitor.executeGlobalNotify(
                GlobalNotificationType.ANY_WORKFLOW_CLASS_STATS_CHANGE,
                new NativeWorkflowClassStats(wfClassStats));
    }

    public synchronized void updateJobStartedStats(JobExecutionModel jobModel) {
        // Update the global stats based on this action.
        globalStats.updateJobStartedStats(jobModel);

        // Update the correct job model stats.
        JobClassStats jobClassStats = getJobClassStats(jobModel.getJobName());
        jobClassStats.updateJobStartedStats(jobModel);
          
        sendJobNotifications(jobModel);
    }
    
    public synchronized void updateJobCompletedStats(JobExecutionModel jobModel) {
        globalStats.updateJobCompletedStats(jobModel);
        
        JobClassStats jobClassStats = getJobClassStats(jobModel.getJobName());
        jobClassStats.updateJobCompletionStats(jobModel);
        
        sendJobNotifications(jobModel);
    }
    
    private synchronized void sendJobNotifications(JobExecutionModel jobModel) {
        monitor.executeGlobalNotify(GlobalNotificationType.GLOBAL_STATS_CHANGE, 
                new NativeGlobalStats(globalStats));
        
        JobClassStats jobClassStats = getJobClassStats(jobModel.getJobName());
        monitor.executeJobClassNotify(new NativeJobClassStats(jobClassStats));
        monitor.executeGlobalNotify(
                GlobalNotificationType.ANY_JOB_CLASS_STATS_CHANGE, 
                new NativeJobClassStats(jobClassStats));
    }
    
    public synchronized void updateResourceThrottledJobLockWaitTime(ResourceThrottledJobModel jobModel) {
        JobClassStats jobClassStats = getJobClassStats(jobModel.getJobName());
        jobClassStats.updateResourceThrottledJobLockWaitTime(jobModel);      
        
        monitor.executeJobClassNotify(new NativeJobClassStats(jobClassStats));
    }
    
    //*************** Accessors to statistical objects        ***************//
     
    public synchronized NativeGlobalStats getGlobalStatsCopy() {
        return new NativeGlobalStats(globalStats);
    }
    
    public synchronized List<String> getWorkflowClassIds() {
        return new ArrayList<String>(workflowClassStats.keySet());
    }
    
    public synchronized NativeWorkflowClassStats getWorkflowClassStatsById(String wfRootJobName) {
        WorkflowClassStats wfClass = workflowClassStats.get(wfRootJobName);
        if (wfClass == null) {
            logger.info("No class stats found for workflow rootClass: " + wfRootJobName);
            return null;
        }
        return new NativeWorkflowClassStats(workflowClassStats.get(wfRootJobName));
    }
    
    public synchronized Map<String, NativeWorkflowClassStats> getAllWorkflowClassStats() {
        Map<String, NativeWorkflowClassStats> allStats = new HashMap<String, NativeWorkflowClassStats>();
        for (Entry<String, WorkflowClassStats> entry : workflowClassStats.entrySet()) {
            allStats.put(entry.getKey(), new NativeWorkflowClassStats(entry.getValue()));
        }
        return allStats;
    }
    
    public synchronized List<String> getJobClassIds() {
        return new ArrayList<String>(jobClassStats.keySet());
    }
    
    public synchronized NativeJobClassStats getJobClassStatsById(String jobClassId) {
        JobClassStats jobClass = jobClassStats.get(jobClassId);
        if (jobClass == null) {
            logger.info("No job class stats exists for job: " + jobClassId);
            return null;
        }
        return new NativeJobClassStats(jobClass);
    }
    
    public synchronized Map<String, NativeJobClassStats> getAllJobClassStats() {
        Map<String, NativeJobClassStats> allStats = new HashMap<String, NativeJobClassStats>();
        for (Entry<String, JobClassStats> entry : jobClassStats.entrySet()) {
            allStats.put(entry.getKey(), new NativeJobClassStats(entry.getValue()));
        }
        return allStats;
    }
    
    private synchronized WorkflowClassStats getWorkflowClassStats(String rootJobName) {
        WorkflowClassStats wfClass = workflowClassStats.get(rootJobName);
        if (wfClass == null) {
            wfClass = new WorkflowClassStats(rootJobName);
            workflowClassStats.put(rootJobName, wfClass);
        }  
        return wfClass;
    }
    
    private synchronized JobClassStats getJobClassStats(String jobName) {
        JobClassStats jobClass = jobClassStats.get(jobName);
        if (jobClass == null) {
            jobClass = new JobClassStats(jobName);
            jobClassStats.put(jobName, jobClass);
        }
        return jobClass;
    }
    
    public synchronized long getNumberOfWorkflows() {
        return wfModels.size();
    }
    
    public synchronized long getNumberOfCompletedWorkflows() {
        long counter = 0;
        for (WorkflowExecutionModel wfModel : wfModels.values()) {
            if (wfModel.isCompleted()) {
                counter++;
            }
        }
        return counter;
    }
    
    public synchronized List<WorkflowExecutionModel> getCompletedWorkflowModels(long endedBeforeTime, 
                                                                                boolean reverseTime) {
        List<WorkflowExecutionModel> workflowModels = new ArrayList<WorkflowExecutionModel>();
        for (WorkflowExecutionModel wfModel : wfModels.values()) {
            if (wfModel.isCompleted() && wfModel.getEndTime() <= endedBeforeTime) {
                workflowModels.add(wfModel);
            }
        }
        Collections.sort(workflowModels, new WFModelsComparator(reverseTime));
        return workflowModels;
    }
    
    public synchronized List<WorkflowExecutionModel> getCompletedWorkflowModels(boolean reverseTime) {
        return getCompletedWorkflowModels(DateTimeUtils.currentTimeMillis(), reverseTime);
    }
    
    public synchronized void clearCompletedWorkflows(long endedBeforeTime) {
        List<WorkflowExecutionModel> completedModels = getCompletedWorkflowModels(endedBeforeTime, false);
        for (WorkflowExecutionModel wfModel : completedModels) {
            wfModels.remove(wfModel.getWorkflowId());
        }
    }
    
    public synchronized void clearCompletedWorkflows() {
        clearCompletedWorkflows(DateTimeUtils.currentTimeMillis());
    }
    
    /**
     * Comparator class used to sort a list of WorkflowExecutionModel's
     * in reverse time order.
     *
     */
    private static class WFModelsComparator implements Comparator<WorkflowExecutionModel> {
        
        private boolean reverse;
        
        /**
         * Constructor
         * @param reverse
         *            indicates (true) if reversing time order in comparison.
         */
        public WFModelsComparator(boolean reverse) {
            this.reverse = reverse;
        }

        @Override
        public int compare(WorkflowExecutionModel arg0, WorkflowExecutionModel arg1) {
            int compare = (arg0.getStartTime() < arg1.getStartTime() ? -1 : 
                           arg0.getStartTime() == arg1.getStartTime() ? 0 : 1);
            return (compare == 0 ? 0 : reverse ? -compare : compare);
        }
        
    }
}
