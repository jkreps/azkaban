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

import azkaban.common.jobs.Job;
import azkaban.jobcontrol.impl.jobs.ResourceThrottledJob;
import azkaban.monitor.MonitorInterface.GlobalNotificationType;
import azkaban.monitor.MonitorInterface.JobState;
import azkaban.monitor.MonitorInterface.WorkflowState;
import azkaban.monitor.stats.ClassStats;
import azkaban.monitor.stats.NativeJobClassStats;
import azkaban.monitor.stats.NativeWorkflowClassStats;

/**
 * Monitor interface used internally by Azkaban - primary use is to field events
 * from the scheduler regarding the launching of workflows and jobs, and handing
 * those events to the execution model, and to statistics aggregation.
 */
public interface MonitorInternalInterface {
    
    /**
     * Enum regarding the types of workflow actions or events that are of 
     * interest capturing.
     */
    public static enum WorkflowAction {
        SCHEDULE_WORKFLOW,
        START_WORKFLOW,
        END_WORKFLOW,
        UNSCHEDULE_WORKFLOW,
    }
    
    /**
     * Enum regarding the types of job actions or events that are of
     * interest in capturing.
     *
     */
    public static enum JobAction {
        START_WORKFLOW_JOB,
        END_WORKFLOW_JOB,        
    }

    /**
     * Captures a pure workflow event, as found in the scheduler typically.
     * @param wfId
     *            String variant of int workflow id.
     * @param time
     *            Time in epoch ms for this event.
     * @param action
     *            Workflow action for this event.
     * @param wfState  
     *            workflow state (only used when action is EndJob, otherwise Nop expected.)
     * @param rootJobName  
     *            name of root workflow job (only used when action is ScheduleJob.)
     */
    public void workflowEvent(String wfId,  
                              long time, 
                              WorkflowAction action, 
                              WorkflowState wfState,
                              String rootJobName);
    
    /**
     * Captures a job event as typically found in executable workflow class 
     * types.
     * @param job
     *            job behind the job event.
     * @param time - 
     *            Time in epoch ms relevant to this event.
     * @param jobAction
     *            job action for this event.
     * @param jobState  
     *            job state (only used when action is EndWorkflow, otherwise Nop expected.)
     */
    public void jobEvent(Job job,
                         long time,            
                         JobAction jobAction, 
                         JobState jobState);
    
    /**
     * Captures the wait time on getting resource locks.
     * @param job
     *            resource throttled job.
     * @param lockWaitTime
     *            time in ms of time logged waiting on locks.
     */
    public void workflowResourceThrottledJobEvent(ResourceThrottledJob job,
                                                  long lockWaitTime);
    
    /**
     * Method to execute the notification system that updates have occurred
     * for the Global Stats, or that some workflow or job statistics object has
     * been updated.
     * @param eventType
     *            GlobalNotificationType of the notify action taken.
     * @param statsObject 
     *            the stats object that changed, global, workflow, or job depending on 
     *            event type.
     */
    public void executeGlobalNotify(GlobalNotificationType eventType, ClassStats statsObject);
    
    /**
     * Method to execute the notification system that a specifc workflow class 
     * statistics object has been updated.
     * @param wfStats 
     *            NativeWorkflowClassStats that will be the argument to the notification.
     */
    public void executeWorkflowClassNotify(NativeWorkflowClassStats wfStats);
    
    /**
     * Method to execute the notification system that a specific job class
     * statistics object has been updated.
     * @param jobStats 
     *            NativeJobClassStats that will be the argument to the notification.
     */
    public void executeJobClassNotify(NativeJobClassStats jobStats);
}
