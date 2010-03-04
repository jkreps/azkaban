package azkaban.common.jobs;

import org.apache.log4j.Logger;

public abstract class AbstractJob implements Job {

    private final String _id;
    private final Logger _log;
    private volatile double _progress;

    protected AbstractJob(String id) {
        _id = id;
        _log = Logger.getLogger(_id);
        _progress = 0.0;
    }

    public String getId() {
        return _id;
    }

    public double getProgress() throws Exception {
        return _progress;
    }

    public void setProgress(double progress) {
        this._progress = progress;
    }

    public void cancel() throws Exception {
        throw new RuntimeException("Job " + _id + " does not support cancellation!");
    }

    public Logger getLog() {
        return this._log;
    }

    public void debug(String message) {
        this._log.debug(message);
    }

    public void debug(String message, Throwable t) {
        this._log.debug(message, t);
    }

    public void info(String message) {
        this._log.info(message);
    }

    public void info(String message, Throwable t) {
        this._log.info(message, t);
    }

    public void warn(String message) {
        this._log.warn(message);
    }

    public void warn(String message, Throwable t) {
        this._log.warn(message, t);
    }

    public void error(String message) {
        this._log.error(message);
    }

    public void error(String message, Throwable t) {
        this._log.error(message, t);
    }

}
