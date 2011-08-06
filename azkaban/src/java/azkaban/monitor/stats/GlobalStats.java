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
 */package azkaban.monitor.stats;

import org.apache.log4j.Logger;

import azkaban.monitor.model.DelegatingJobModel;
import azkaban.monitor.model.JobExecutionModel;
import azkaban.monitor.model.WorkflowExecutionModel;

/**
 * Global statistics about what is running in Azkaban generally, e.g. number of jobs.
 */
public class GlobalStats extends NativeGlobalStats {
    private static final Logger logger = Logger.getLogger(GlobalStats.class);
    
    /**
     * Constructor.     
     */
    public GlobalStats() {
        super();
        startTime = System.currentTimeMillis();
    }
    
    /**
     * Update global statistics based on the scheduling of a workflow.
     * @param wfModel
     *            the workflow execution that was scheduled. 
     */
    public synchronized void updateWorkflowScheduledStats(WorkflowExecutionModel wfModel) {
        totalWorkflowsScheduled++;
        totalWorkflowsPending = totalWorkflowsScheduled - totalWorkflowsStarted;
    }
    
    /**
     * Update global statistics based on the starting of a workflow.
     * @param wfModel
     *            the workflow model that started.
     */
    public synchronized void updateWorkflowStartedStats(WorkflowExecutionModel wfModel) {
        totalWorkflowsStarted++;
        totalWorkflowsPending = totalWorkflowsScheduled - totalWorkflowsStarted;
        highFlowId = Math.max(highFlowId, Long.parseLong(wfModel.getWorkflowId()));
    }
    
    @Override
    public final Object clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException();
    }
    
    /**
     * Update global statistics based on workflow end.
     * @param wfModel
     *        the workflow execution which ended.
     */
    public synchronized void updateWorkflowEndedStats(WorkflowExecutionModel wfModel) {
        switch (wfModel.getCompletionState()) {
        case SUCCESSFUL:
            totalWorkflowsSuccessful++;
            break;
        case FAILED:
            totalWorkflowsFailed++;
            break;
        case CANCELED:
            totalWorkflowsCanceled++;
            break;
        default:
            logger.debug("Unknown WorkflowState.");
            break;
        }
    }
    
    /**
     * Update global statistics based on job started.
     * @param jobModel
     *            the job executiobn that started.
     */
    public synchronized void updateJobStartedStats(JobExecutionModel jobModel) {
        // Don't count delegated activities, as they mis-lead.  You can wind up 
        // double or triple counting actual starts - only the inner-most jobs.
        if (jobModel instanceof DelegatingJobModel) {
            return;
        }
        totalJobsStarted++;
    }
    
    /**
     * Update global statistics based on job completed.
     * @param jobModel
     *            the job execution that completed.
     */
    public synchronized void updateJobCompletedStats(JobExecutionModel jobModel) {
        // Don't count delegated activities, as they mis-lead.  You can wind up 
        // double or triple counting actual starts - only the inner-most jobs.
        if (jobModel instanceof DelegatingJobModel) {
            return;
        }
        switch (jobModel.getCompletionState()) {
        case SUCCESSFUL:
            totalJobsSuccessful++;
            break;
        case FAILED:
            totalJobsFailed++;
            break;
        case CANCELED:
            totalJobsCanceled++;
            break;
        default:
            logger.debug("Unknown JobState.");
            break;
        }
    }  
}
