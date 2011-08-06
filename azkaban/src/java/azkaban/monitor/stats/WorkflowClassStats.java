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

import org.apache.commons.math.stat.descriptive.SynchronizedSummaryStatistics;

import azkaban.monitor.model.WorkflowExecutionModel;

/**
 * For a given job root name, keep track of statistics for all workflows that  
 * originate from jobs with that name.
 */
public class WorkflowClassStats extends NativeWorkflowClassStats {
    
    /**
     * Math structures to compute averages and stdvar.for pending.
     */
    private SynchronizedSummaryStatistics normalWfPendingStats = new SynchronizedSummaryStatistics();
    
    /**
     * Math structures to compute averages and stdvar. for runtime.
     */
    private SynchronizedSummaryStatistics normalWfRunStats = new SynchronizedSummaryStatistics();
    
    /**
     * Math structures to compute averages and stdvar. for runtimes of failed workflows.
     */
    private SynchronizedSummaryStatistics failedWfRunStats = new SynchronizedSummaryStatistics();
    
    /**
     * Math structures to compute averages and stdvar. for runtimes of canceled workflows.
     */
    private SynchronizedSummaryStatistics canceledWfRunStats = new SynchronizedSummaryStatistics();
    
    /**
     * Constructor used to build the workflow class
     * @param workflowRootName
     *            workflow root job name
     */
    public WorkflowClassStats(String workflowRootName) {
        super();
        this.workflowRootName = workflowRootName;
    }
    
    /**
     * Update statistics based on the scheduling of a workflow.
     * @param wfModel
     *            workflow execution behind scheduling stats.
     */
    public synchronized void updateWorkflowScheduledStats(WorkflowExecutionModel wfModel) {
        numTimesWorkflowScheduled++;
    }
    
    /**
     * Update statistics based on the starting of a workflow.
     * @param wfModel
     *            workflow exection behind starting stats.
     */
    public synchronized void updateWorkflowStartedStats(WorkflowExecutionModel wfModel) {
        numTimesWorkflowStarted++;
        if (lastWorkflowTimeStarted == 0 || 
                lastWorkflowTimeStarted < wfModel.getStartTime()) {
            lastWorkflowTimeStarted = wfModel.getStartTime();
        }
        
        long incTime = wfModel.getPendingTime();
        normalWfPendingStats.addValue(incTime);
        avgWorkflowPendingTime = normalWfPendingStats.getMean();
        stdWorkflowPendingTime = normalWfPendingStats.getStandardDeviation();
    }
    
    /**
     * Update workflow statistics based on the ending of a workflow.
     * @param wfModel
     *            the workflow execution behind workflow ending stats.
     */
    public synchronized void updateWorkflowEndedStats(WorkflowExecutionModel wfModel) {
        SynchronizedSummaryStatistics wfStats = null;
        switch (wfModel.getFinalWorkflowState()) {
        case SUCCESSFUL:
            numTimesWorkflowSuccessful++;
            wfStats = normalWfRunStats;
            break;
        case FAILED:
            numTimesWorkflowFailed++;
            wfStats = failedWfRunStats;
            break;
        case CANCELED:
            numTimesWorkflowCanceled++;
            wfStats = canceledWfRunStats;
            break;
         default:
             return;
        }

        /**
         * We are only keeping averages and std dev on successful workflows.
         */
        long incTime = wfModel.getEndTime() - wfModel.getStartTime();
        wfStats.addValue(incTime);
        double avg = wfStats.getMean();
        double std = wfStats.getStandardDeviation();
        switch(wfModel.getFinalWorkflowState()) {
        case SUCCESSFUL:
            avgWorkflowRunTime = avg;
            stdWorkflowRunTime = std;
            break;
        case FAILED:
            avgWorkflowFailedTime = avg;
            stdWorkflowFailedTime = std;
            break;
        case CANCELED:
            avgWorkflowCanceledTime = avg;
            stdWorkflowCanceledTime = std;
            break;
        }
    }
}
