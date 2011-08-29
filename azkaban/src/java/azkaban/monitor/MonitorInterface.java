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
 */package azkaban.monitor;

import java.util.List;
import java.util.Map;

import azkaban.monitor.model.WorkflowExecutionModel;
import azkaban.monitor.stats.NativeGlobalStats;
import azkaban.monitor.stats.NativeJobClassStats;
import azkaban.monitor.stats.NativeWorkflowClassStats;

/**
 * This is the interface to the monitor system that should be used by application clients 
 * that want to acquire job run statistics.  It consists of the following:
 *     1) A set of Global, Workflow, and Job state enums
 *     2) A set of accessors to Global, Workflow, and Job statistics objects.
 *     3) A set of registration notification methods to enable state update notification.
 *
 */
public interface MonitorInterface {
    
    /**
     *  Job termination states.
     */
    public static enum JobState {
        /**
         * Job successful
         */
        SUCCESSFUL,
        /**
         * Job failed
         */
        FAILED,
        /**
         * Job canceled
         */
        CANCELED,
        /**
         * Job unknown
         */
        UNKNOWN,
        /**
         * Use this value when JobState irrelevant to the event, e.g. start job.
         */
        NOP,
    }
    
    /**
     * Workflow termination states.
     */
    public static enum WorkflowState {
        /**
         * Workflow successful
         */
        SUCCESSFUL,
        /**
         * Workflow failed
         */
        FAILED,
        /**
         * Workflow canceled
         */
        CANCELED,
        /**
         * Workflow unknown
         */
        UNKNOWN,
        /**
         * Use this value when WorkflowState is irrelevant to the event, e.g. start workflow.
         */
        NOP,
    }
    
    /**
     * Global notification enablement enums.
     */
    public static enum GlobalNotificationType {
        /**
         * Indicates global stats over server.
         */
        GLOBAL_STATS_CHANGE,
        /**
         * Indicates aggregate workflow stats based on root job name.
         */
        ANY_WORKFLOW_CLASS_STATS_CHANGE,
        /**
         * Indicates aggregate job stats based on job name.
         */
        ANY_JOB_CLASS_STATS_CHANGE,
    }
    
    //*****  Global Statistics accessor methods                         *****//
    
    /**
     * Get a copy of the global azkaban statistics.
     * @return NativeGlobalStats a copy.
     */
     NativeGlobalStats getGlobalAzkabanStats();
    
    /**
     * Get a list of all the current workflow as id'd about root job name.
     * @return List<String> of all job names serving as a root of a workflow.
     */
    List<String> getWorkflowClassRootJobNames();
    
    /**
     * Get a copy of a WorkflowClassStats based on a given root job name.
     * @param rootJobName
     *            the root job name on which we search for the workflow class stats.
     * @return NativeWorkflowClassStats a copy if found based on classId.
     */
    NativeWorkflowClassStats getWorkflowClassStatsByRootJobName(String rootJobName);
    
    /**
     * Get a copy of all the current workflow class stats objects.
     * @return Map<String, NativeWorkflowClassStats> 
     *             mapping job root name to workflow class stats.
     */
    Map<String, NativeWorkflowClassStats> getAllWorkflowClassStats();
    
    /**
     * Get a list of all the Job Classes available.
     * @return List<String> listing all job names whose stats are being collected.
     */
    List<String> getJobClassNames();
    
    /**
     * Get a copy of the JobClassStats as given by the job class name.
     * @param jobClassName
     *            the job id on which we are searching for the job stats
     * @return NativeJobClassStats a copy if found.
     */
    NativeJobClassStats getJobClassStatsByName(String jobClassName);
    
    /**
     * Get set of all JobClassStats.  This will be a full copy, not the originals.
     * @return Map<String, NativeJobClassStats> mapping the name of the job
     *             to its NativeJobClassStats. 
     */
    Map<String, NativeJobClassStats> getAllJobClassStats();  
    
    /**
     * Return the number of workflows, active and completed.
     * @return the number of workflow instances that are being monitored.
     */
    long getNumberOfWorkflows();
    
    /**
     * Return the number of completed workflows.
     * @return long value for the number of completed workflow 
     *         instances that were monitored.
     */
    long getNumberOfCompletedWorkflows();
    
    /**
     * Return a list of all currently completed workflow models.
     * @param reverseTime - boolean to indicate reverse order.
     * @return List<WorkflowExecutionModel> - a list of all workflow model instances
     *     that completed. 
     */
    List<WorkflowExecutionModel> getCompletedWorkflowModels(boolean reverseTime);
    
    /**
     * Return a list of workflow models completed prior to a given time.
     * @param endedBeforeTime
     *           epoch time before which workflows must end.
     * @param reverseTime
     *           boolean true indicates return list in reverse time order.
     * @return
     *     List<WorkflowExecutionModel>
     *     
     */
    List<WorkflowExecutionModel> getCompletedWorkflowModels(
                                             long endedBeforeTime, 
                                             boolean reverseTime);
    
    /**
     * Clear out all completed workflow models that ended some amount
     * of time earlier than current time.
     * @param endedBeforeTime
     *            epoch time before which workflows must end.
     */
    void clearCompletedWorkflows(long endedBeforeTime);
    
    /**
     * Clear out all the completed workflow models - used to free storage.
     */
    void clearCompletedWorkflows();
    
    //*****  Notification Methods                                       *****//
    
    /**
     * Register notifier for global stats, or any workflow class or job class changes.
     * @param notifier 
     *            notification interface that should be registered.
     * @param type 
     *            based on GlobalNotificationType, the type of notification requested.
     */
    void registerGlobalNotification(MonitorListener notifier,
                                    GlobalNotificationType type);  
    
    /**
     * Deregister notifier for a type of global notification.
     * @param notifier 
     *            notification interface that should be deregistered.
     * @param type 
     *            based on GlobalNotificationType, the type of notification to deregister.
     */
    void deregisterGlobalNotification(MonitorListener notifier,
                                      GlobalNotificationType type);
    

    /**
     * Register for changes on a particular workflow class, given by root job name.
     * @param notifier
     *            notifier to be registered
     * @param workflowRootJobName 
     *            name of the workflow root job on which to be registered.
     */
    void registerWorkflowClassNotification(MonitorListener notifier,
                                           String workflowRootJobName);
    
    /**
     * Deregister a notifier for notifications on a workflow class.
     * @param notifier 
     *            notifier to be deregistered.
     * @param workflowRootJobName
     *            name of workflow root job on which to deregister.
     */
    void  deregisterWorkflowClassNotification(MonitorListener notifier,
                                              String workflowRootJobName);
  
    /**
     * Register for changes on a particular job class - as indicated by job name.
     * @param notifier 
     *            notifier to be registered.
     * @param jobClassName
     *            job class name on which to register.
     */
    void registerJobClassNotification(MonitorListener notifier,
                                      String jobClassName);
    
    /**
     * Deregister the notifier for job class notification on some job class
     * @param notifier
     *            notifier to be deregistered.
     * @param jobClassName
     *            job class name on which to deregister.
     */
    void deregisterJobClassNotification(MonitorListener notifier,
                                        String jobClassName);
    
    /**
     * Unregister this notifier from all its registered notifications.
     * @param notifier 
     *            notifier to be deregistered.
     */
    void deregisterNotifications(MonitorListener notifier);
}
