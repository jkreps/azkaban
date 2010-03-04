package azkaban.jobcontrol.impl.jobs;

public enum JobStatus {
    WAITING,
    READY,
    RUNNING,
    SUCCESS,
    FAILED,
    DEPENDENT_FAILED,
}