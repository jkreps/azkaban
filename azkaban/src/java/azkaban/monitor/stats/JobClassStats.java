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
import org.apache.log4j.Logger;

import azkaban.monitor.MonitorInterface.JobState;
import azkaban.monitor.model.JobExecutionModel;
import azkaban.monitor.model.LoggingJobModel;
import azkaban.monitor.model.ResourceThrottledJobModel;
import azkaban.monitor.model.RetryingJobModel;

/**
 * Class that collect global statistics over the runs of a particular job class.
 * This class includes statistics associated with the job class if it incorporates retries,
 * resource throttling, and logging.
 * Statistics are of the nature of run counts, runtime averages, and many other counts
 * and statistics.
 *
 */
public class JobClassStats extends NativeJobClassStats {
    private static final Logger logger = Logger.getLogger(JobClassStats.class);
    
    /**
     * Stats mechanism for all non-delegating successful jobs.
     */
    private SynchronizedSummaryStatistics normalJobStats = new SynchronizedSummaryStatistics();
    
    /**
     * Stats mechanism for all non-delegating failed jobs.
     */
    private SynchronizedSummaryStatistics failedJobStats = new SynchronizedSummaryStatistics();
    
    /**
     * Stats mechanism for all non-delegating canceled jobs.
     */
    private SynchronizedSummaryStatistics canceledJobStats = new SynchronizedSummaryStatistics();
    
    /**
     * Stats mechanism for all retry jobs.
     */
    private SynchronizedSummaryStatistics retryJobStats = new SynchronizedSummaryStatistics();
    
    /**
     * Stats mechanism for all job retries.
     */
    private SynchronizedSummaryStatistics retryJobRetriesStats = new SynchronizedSummaryStatistics();
    
    /**
     * Stats mechanism for all resource throttled jobs.
     */
    private SynchronizedSummaryStatistics resourceThrottledJobStats = new SynchronizedSummaryStatistics();
    
    /**
     * Primary constructor for JobClassStats.
     * @param jobClassName
     *            the job class name for these statistics.
     */
    public JobClassStats(String jobClassName) {
        super();
        this.jobClassName = jobClassName;
    }
    
    /**
     * Update job statistics based on a job starting.
     * @param jobModel
     *            the job execution model used to update started stats.
     */
    public synchronized void updateJobStartedStats(JobExecutionModel jobModel) {
        if (jobModel instanceof RetryingJobModel) {
            retryJob = true;
            numRetryJobStarts++;
        } else if (jobModel instanceof ResourceThrottledJobModel) {
            resourceThrottledJob = true;
            numResourceThrottledStarted++;
        } else if (jobModel instanceof LoggingJobModel) {
            loggingJob = true;
            numLoggingJobStarts++;
        } else {
            numTimesJobStarted++;
        }
        if (lastTimeJobStarted == 0 || 
                lastTimeJobStarted < jobModel.getStartTime()) {
                    lastTimeJobStarted = jobModel.getStartTime();
        }
    }
    
    /**
     * Update job statistics based on job ending.
     * @param jobModel
     *            the job execution model used to update completion stats.
     */
    public synchronized void updateJobCompletionStats(JobExecutionModel jobModel) {
        if (jobModel instanceof RetryingJobModel) {
            updateRetryJobCompletionStats((RetryingJobModel)jobModel);
        } else if (!(jobModel instanceof ResourceThrottledJobModel) &&
                   !(jobModel instanceof LoggingJobModel)) {
            updateNormalJobCompletionStats(jobModel);
        }
    }
    
    /**
     * Update job statistics based on a retry job.
     * @param jobModel
     *            the retrying model used to update retry completion stats.
     */
    private synchronized void updateRetryJobCompletionStats(RetryingJobModel jobModel) {
        switch (jobModel.getFinalJobState()) {
        case SUCCESSFUL:
            numRetryJobSuccessful++;
            break;
        case FAILED:
            numRetryJobFailures++;
            break;
        case CANCELED:
            numRetryJobCanceled++;
            break;
        default:
            logger.debug("Unknown JobState.");
            return;
        }
        
        /**
         * Only take average and std dev for successful jobs.
         */
        if (jobModel.getFinalJobState() == JobState.SUCCESSFUL) {
            
            long incTime = jobModel.getEndTime() - 
                           jobModel.getStartTime();            
            retryJobStats.addValue(incTime);
            avgRetryJobRunTime = retryJobStats.getMean();
            stdRetryJobRunTime = retryJobStats.getStandardDeviation();  
        } 
        
        retryJobRetriesStats.addValue(jobModel.getNumberOfTries());
        numJobTries = (long)retryJobRetriesStats.getSum();
        avgNumJobTries = retryJobRetriesStats.getMax();
    }
    
    /**
     * Update a non delegating job's statistics.
     * @param jobModel
     *            the job execution model used to update job completion stats.
     */
    private void updateNormalJobCompletionStats(JobExecutionModel jobModel) {
        SynchronizedSummaryStatistics jobStats = null;
        switch (jobModel.getFinalJobState()) {
        case SUCCESSFUL:
            numTimesJobSuccessful++;
            jobStats = normalJobStats;
            break;
        case FAILED:
            numTimesJobFailed++;
            jobStats = failedJobStats;
            break;
        case CANCELED:
            numTimesJobCanceled++;
            jobStats = canceledJobStats;
            break;
        default:
            logger.debug("Unknown JobState.");
            return;
        }
        
        /**
         * Take average and std dev for the different job states.
         */
        long incTime = jobModel.getEndTime() - 
                                jobModel.getStartTime();  
        jobStats.addValue(incTime);
        double avg = jobStats.getMean();
        double std = jobStats.getStandardDeviation();
        switch(jobModel.getFinalJobState()) {
        case SUCCESSFUL:
            avgJobRunTime = avg;
            stdJobRunTime = std;  
            break;
        case FAILED:
            avgJobFailedTime = avg;
            stdJobFailedTime = std; 
            break;
        case CANCELED:
            avgJobCanceledTime = avg;
            stdJobCanceledTime = std;
            break;
        }
    }
    
    /**
     * Update the resource throttled job wait time on locks.
     * @param jobModel
     *            the resource throttled job model used to update resource wait times.
     */
    public void updateResourceThrottledJobLockWaitTime(ResourceThrottledJobModel jobModel) {
        resourceThrottledJobStats.addValue(jobModel.getLockWaitTime());
        totalResourceThrottledWaitTime = resourceThrottledJobStats.getSum();
        avgResourceThrottledWaitTime = resourceThrottledJobStats.getMean();
        stdResourceThrottledWaitTime = resourceThrottledJobStats.getStandardDeviation();
    }

}
