package azkaban.jobs;

import azkaban.common.utils.Props;

public interface Executor {

    /**
     * Returns a unique(should be checked in xml) string name/id for the Job.
     * 
     * @return
     */
    public String getId();

    /**
     * Run the job. In general this method can only be run once. Must either
     * succeed or throw an exception.
     */
    public void run() throws Exception;

    /**
     * Best effort attempt to cancel the job.
     * 
     * @throws Exception If cancel fails
     */
    public void cancel() throws Exception;

    /**
     * Returns a progress report between [0 - 1.0] to indicate the percentage
     * complete
     * 
     * @throws Exception If getting progress fails
     */
    public double getProgress() throws Exception;
    
    /**
     * Get the generated properties from this job.
     * @return
     */
    public Props getJobGeneratedProperties();
}