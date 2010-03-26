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

package azkaban.app;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.ReadablePeriod;

import azkaban.common.utils.Utils;

/**
 * A Job instance decorated with schedule information.
 * 
 * ScheduledJobs are Runnable so they can be feed directly into a thread pool
 * 
 * TODO: We should be able to merge this with JobExecution
 * 
 * @author jkreps
 * 
 */
public class ScheduledJob {

    private final String _jobName;
    private final ReadablePeriod _period;
    private final DateTime _nextScheduledExecution;
    private final boolean _ignoreDependency;
    private volatile DateTime _started;
    private volatile DateTime _ended;
    private volatile boolean _invalid = false;
    private volatile Runnable _runnable = null;

    public ScheduledJob(String jobName,
                        JobManager jobManager,
                        DateTime nextExecution,
                        boolean ignoreDependency) {
        this(jobName, nextExecution, null, ignoreDependency);
    }

    public ScheduledJob(String jobName,
                        DateTime nextExecution,
                        ReadablePeriod period,
                        boolean ignoreDependency) {
        super();
        _ignoreDependency = ignoreDependency;
        _jobName = Utils.nonNull(jobName);
        _period = period;
        _nextScheduledExecution = Utils.nonNull(nextExecution);
    }

    public String getId() {
        return this._jobName;
    }

    public boolean isRecurring() {
        return this._period != null;
    }

    public ReadablePeriod getPeriod() {
        return _period;
    }

    public DateTime getScheduledExecution() {
        return _nextScheduledExecution;
    }

    public DateTime getStarted() {
        return _started;
    }

    public DateTime getEnded() {
        return _ended;
    }

    public void setStarted(DateTime date) {
        this._started = date;
    }

    public void setEnded(DateTime date) {
        this._ended = date;
    }

    public Duration getExecutionDuration() {
        if(_started == null || _ended == null)
            throw new IllegalStateException("Job has not completed yet.");
        return new Duration(_started, _ended);
    }

    public boolean isDependencyIgnored() {
        return _ignoreDependency;
    }

    public void setScheduledRunnable(Runnable runnable) {
        _runnable = runnable;
    }

    public Runnable getScheduledRunnable() {
        return _runnable;
    }

    public void markInvalid() {
        _invalid = true;
    }

    public boolean isInvalid() {
        return _invalid;
    }

    @Override
    public String toString()
    {
        return "ScheduledJob{" +
               "_ignoreDependency=" + _ignoreDependency +
               ", _nextScheduledExecution=" + _nextScheduledExecution +
               ", _period=" + _period +
               ", _jobName='" + _jobName + '\'' +
               ", _invalid=" + _invalid +
               '}';
    }
}
