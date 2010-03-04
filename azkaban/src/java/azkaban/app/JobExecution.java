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
