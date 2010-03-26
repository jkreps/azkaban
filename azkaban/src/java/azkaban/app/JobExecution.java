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

/**
 * Represents information about the execution of a job
 * 
 * @author jkreps
 * 
 */
public class JobExecution {

    private final String _jobName;
    private final DateTime _start;
    private final DateTime _end;
    private final boolean _succeeded;
    private final String _log;

    public JobExecution(String jobName, DateTime start, DateTime end, boolean succeeded, String log) {
        super();
        _jobName = jobName;
        _start = start;
        _end = end;
        _succeeded = succeeded;
        _log = log;
    }

    public String getJobName() {
        return _jobName;
    }

    public DateTime getStarted() {
        return _start;
    }

    public DateTime getEnded() {
        return _end;
    }

    public boolean hasEnded() {
        return _end != null;
    }

    public boolean isSucceeded() {
        return _succeeded;
    }

    public String getLog() {
        return _log;
    }
}
