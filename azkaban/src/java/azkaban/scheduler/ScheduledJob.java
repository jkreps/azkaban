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

package azkaban.scheduler;

import org.joda.time.DateTime;
import org.joda.time.ReadablePeriod;

import azkaban.common.utils.Utils;


/**
 * Schedule for a job instance. This is decoupled from the execution.
 * 
 * @author Richard Park
 * 
 */
public class ScheduledJob {

    private final String jobName;
    private final ReadablePeriod period;
    private DateTime nextScheduledExecution;
    private final boolean ignoreDependency;

    /**
     * Constructor 
     * 
     * @param jobName Unique job name
     * @param nextExecution The next execution time
     * @param ignoreDependency 
     */
    public ScheduledJob(String jobName,
                        DateTime nextExecution,
                        boolean ignoreDependency) {
        this(jobName, nextExecution, null, ignoreDependency);
    }

    /**
     * Constructor
     * 
     * @param jobId
     * @param nextExecution
     * @param period
     * @param ignoreDependency
     */
    public ScheduledJob(String jobId,
                        DateTime nextExecution,
                        ReadablePeriod period,
                        boolean ignoreDependency) {
        super();
        this.ignoreDependency = ignoreDependency;
        this.jobName = Utils.nonNull(jobId);
        this.period = period;
        this.nextScheduledExecution = Utils.nonNull(nextExecution);
    }
    
    /**
     * Updates the time to a future time after 'now' that matches the period description.
     * 
     * @return
     */
    public boolean updateTime() {
    	if (nextScheduledExecution.isAfterNow()) {
    		return true;
    	}
    	
        if (period != null) {
        	DateTime other = getNextRuntime(nextScheduledExecution, period);
    		
    		this.nextScheduledExecution = other;
    		return true;
    	}

        return false;
    }
    
    /**
     * Calculates the next runtime by adding the period.
     * 
     * @param scheduledDate
     * @param period
     * @return
     */
    private DateTime getNextRuntime(DateTime scheduledDate, ReadablePeriod period)
    {
        DateTime now = new DateTime();
        DateTime date = new DateTime(scheduledDate);
        int count = 0;
        while (!now.isBefore(date)) {
            if (count > 100000) {
                throw new IllegalStateException("100000 increments of period did not get to present time.");
            }

            if (period == null) {
                break;
            }
            else {
                date = date.plus(period);
            }

            count += 1;
        }

        return date;
    }
    
    /**
     * Returns the unique id of the job to be run.
     * @return
     */
    public String getId() {
        return this.jobName;
    }

    /**
     * Returns true if the job recurrs in the future
     * @return
     */
    public boolean isRecurring() {
        return this.period != null;
    }

    /**
     * Returns the recurrance period. Or null if not applicable
     * @return
     */
    public ReadablePeriod getPeriod() {
        return period;
    }

    /**
     * Returns the next scheduled execution
     * @return
     */
    public DateTime getScheduledExecution() {
        return nextScheduledExecution;
    }

    /**
     * Returns true if the dependency is ignored.
     * @return
     */
    public boolean isDependencyIgnored() {
        return ignoreDependency;
    }

    @Override
    public String toString()
    {
        return "ScheduledJob{" +
               "ignoreDependency=" + ignoreDependency +
               ", nextScheduledExecution=" + nextScheduledExecution +
               ", period=" + period +
               ", jobName='" + jobName + '\'' +
               '}';
    }
}
