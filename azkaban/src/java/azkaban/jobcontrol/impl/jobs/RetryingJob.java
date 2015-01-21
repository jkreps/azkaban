/*
 * Copyright 2010 LinkedIn, Inc
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
 */

package azkaban.jobcontrol.impl.jobs;

import org.apache.log4j.Logger;

import azkaban.common.jobs.DelegatingJob;
import azkaban.common.jobs.Job;
import azkaban.common.jobs.JobFailedException;
import azkaban.monitor.MonitorImpl;
import azkaban.monitor.MonitorInterface.JobState;
import azkaban.monitor.MonitorInternalInterface.JobAction;

public class RetryingJob extends DelegatingJob {

    private final Logger _logger;
    private final int _retries;
    private final long _retryBackoff;
    private volatile boolean _isCancelled;

    public RetryingJob(Job innerJob, int retries, long retryBackoff) {
        super(innerJob);
        _logger = Logger.getLogger(innerJob.getId());
        _retries = retries;
        _retryBackoff = retryBackoff;
    }

    @Override
    public void run() {
        synchronized (this) {
            _isCancelled = false;
        }
        for(int tries = 0; tries <= _retries & !_isCancelled; tries++) {
            // helpful logging info
            if(tries > 0) {
                if(_retryBackoff > 0) {
                    _logger.info("Chillaxing for " + _retryBackoff + " ms until next job retry.");
                    try {
                        Thread.sleep(_retryBackoff);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
                _logger.info("Retrying failed job '" + getInnerJob().getId() + " for attempt " + (tries + 1));
            }
            
            MonitorImpl.getInternalMonitorInterface().jobEvent( 
                    getInnerJob(),
                    System.currentTimeMillis(),
                    JobAction.START_WORKFLOW_JOB,
                    JobState.NOP);

            try {
                getInnerJob().run();
                
                MonitorImpl.getInternalMonitorInterface().jobEvent( 
                        getInnerJob(),
                        System.currentTimeMillis(),
                        JobAction.END_WORKFLOW_JOB,
                        JobState.SUCCESSFUL);
                return;
            } catch(Exception e) {
                _logger.error("Job '" + getInnerJob().getId() + " failed attempt " + (tries + 1), e);
                String sadness = "";
                for(int i = 0; i < tries + 1; i++)
                    sadness += ":-( ";
                _logger.info(sadness);
                
                MonitorImpl.getInternalMonitorInterface().jobEvent( 
                        getInnerJob(),
                        System.currentTimeMillis(),
                        JobAction.END_WORKFLOW_JOB,
                        getInnerJob().isCanceled() ? JobState.CANCELED : JobState.FAILED);
            }
        }

        // if we get here it means we haven't succeded (otherwise we would have
        // returned)
        throw new JobFailedException(_retries + " run attempt" + (_retries > 1 ? "s" : "") +
                " failed.");
    }
    
    public synchronized boolean isCanceled() {
        return getInnerJob().isCanceled();
    }
    
    @Override
    public void cancel() throws Exception {
        synchronized (this) {
            _isCancelled = true;
          }
        getInnerJob().cancel();
    }

}
