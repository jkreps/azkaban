package azkaban.jobcontrol.impl.jobs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import azkaban.app.JobDescriptor;

import azkaban.common.jobs.AbstractJob;
import azkaban.common.jobs.Job;
import azkaban.common.utils.Props;

/**
 * JobGraph defines a way to box a Network of {@link Job} as a single Job. It
 * manages JobDependencies between different Jobs. All Ready jobs are scheduled
 * to RUN in parallel. A Job is blocked on its parents
 * {@link Job#getDependencyJobList()} A job is scheduled after all its parents
 * are {@link JobSTATUS} <imp>Currently Input/Output are explicitly defined for
 * all Job hence Jobs should not be reused. Will overwrite old outputs or
 * fail</imp>
 * 
 * @author bbansal
 *         <p/>
 *         The code here is heavily borrowed from {@link JobController}
 */
public class JobGraph extends AbstractJob {

    public static enum JobSTATUS {
        WAITING,
        READY,
        RUNNING,
        SUCCESS,
        FAILED,
        DEPENDENT_FAILED,
    }

    // The thread can be in one of the following state
    private static final int RUNNING = 0;
    private static final int STOPPED = 2;
    private static final int STOPPING = 3;
    private static final int READY = 4;

    private volatile int runnerState;

    private Map<String, Job> waitingJobs;
    private Map<String, Job> readyJobs;
    private Map<String, Job> runningJobs;
    private Map<String, Job> successfulJobs;
    private Map<String, Job> failedJobs;

    private ConcurrentHashMap<Job, JobSTATUS> _jobStatus;
    private Map<Job, ArrayList<Job>> _jobDependencies;
    private Map<String, JobDescriptor> _descriptors;

    private ExecutorService _threadPool;

    private long nextJobID;
    private String _exceptionString = "";

    private Job WrappedJob = null;

    /**
     * Construct a job control for a group of jobs.
     */
    public JobGraph(String jobID,
                    Props props,
                    ConcurrentHashMap<Job, JobSTATUS> jobStatusMap,
                    Map<String, JobDescriptor> descriptors) {
        super(jobID);

        this.waitingJobs = new Hashtable<String, Job>();
        this.readyJobs = new Hashtable<String, Job>();
        this.runningJobs = new Hashtable<String, Job>();
        this.successfulJobs = new Hashtable<String, Job>();
        this.failedJobs = new Hashtable<String, Job>();
        this._descriptors = descriptors;

        _jobStatus = jobStatusMap;
        _jobDependencies = new Hashtable<Job, ArrayList<Job>>();

        // For Thread Pool
        this._threadPool = Executors.newCachedThreadPool();

        this.nextJobID = -1;
        this.runnerState = JobGraph.READY;
    }

    public List<Job> getWaitingJobList() {
        return new ArrayList<Job>(waitingJobs.values());
    }

    public List<Job> getRunningJobList() {
        return new ArrayList<Job>(runningJobs.values());
    }

    public List<Job> getReadyJobList() {
        return new ArrayList<Job>(readyJobs.values());
    }

    public List<Job> getSuccessJobList() {
        return new ArrayList<Job>(successfulJobs.values());
    }

    public List<Job> getFailedJobList() {
        return new ArrayList<Job>(failedJobs.values());
    }

    public Map<Job, ArrayList<Job>> getJobDependencyList() {
        return new Hashtable<Job, ArrayList<Job>>(_jobDependencies);
    }

    public String getExceptionString() {
        return _exceptionString;
    }

    @Override
    public double getProgress() throws Exception {
        double completed = 1.0 * successfulJobs.size() + 1.0 * failedJobs.size()
                           + sumProgress(waitingJobs.values()) + sumProgress(readyJobs.values())
                           + sumProgress(runningJobs.values());
        double totalJobs = successfulJobs.size() + failedJobs.size() + waitingJobs.size()
                           + readyJobs.size() + runningJobs.size();

        return completed / totalJobs;
    }

    private double sumProgress(Collection<Job> collection) throws Exception {
        double sum = 0.0;
        for(Job job: collection)
            sum += job.getProgress();
        return sum;
    }

    private static void addToQueue(Job aJob, Map<String, Job> queue) {
        synchronized(queue) {
            queue.put(aJob.getId(), aJob);
        }
    }

    private JobSTATUS getJobStatus(Job job) {
        JobSTATUS status = _jobStatus.get(job);
        if(null == status) {
            throw new IllegalStateException("JobStatus not found for Job:" + job.getId());
        }

        return status;
    }

    private void setJobStatus(Job job, JobSTATUS status) {
        if(_jobStatus.get(job) != status) {
            info("SetStatus(JobId:" + getId() + ")  Job:" + job.getId() + " status ("
                 + getJobStatus(job) + " --> " + status + ")");
        }
        _jobStatus.put(job, status);
    }

    private List<Job> getDependencies(Job job) {
        ArrayList<Job> parents = _jobDependencies.get(job);
        if(null == parents)
            return new ArrayList<Job>();
        else
            return parents;
    }

    private void setDependencies(Job job, List<Job> parentJobs) {
        ArrayList<Job> parents = new ArrayList<Job>();

        if(parentJobs != null) {
            for(Job parent: parentJobs) {
                parents.add(parent);
            }
        }

        // Add to HashTable in all cases.
        _jobDependencies.put(job, parents);
    }

    private void addToQueue(Job aJob) {
        Map<String, Job> queue = getQueue(getJobStatus(aJob));
        addToQueue(aJob, queue);
    }

    private Map<String, Job> getQueue(JobSTATUS state) {
        Map<String, Job> retv = null;

        switch(state) {

            case DEPENDENT_FAILED: /* Intentional fall through */
            case FAILED:
                retv = this.failedJobs;
                break;

            case READY:
                retv = this.readyJobs;
                break;

            case RUNNING:
                retv = this.runningJobs;
                break;

            case SUCCESS:
                retv = this.successfulJobs;
                break;

            case WAITING:
                retv = this.waitingJobs;
                break;
        }
        return retv;
    }

    public void addJob(Job aJob, Job... dependencies) {
        addJob(aJob, Arrays.asList(dependencies));
    }

    /**
     * Add a new job.
     * 
     * @param aJob the the new job
     * @throws Exception
     */
    public void addJob(Job aJob, List<Job> dependencies) {
        // Add to waiting_queue
        _jobStatus.putIfAbsent(aJob, JobSTATUS.WAITING);
        this.addToQueue(aJob);

        // Add dependencies into waiting_queues
        for(Job j: dependencies) {
            _jobStatus.putIfAbsent(j, JobSTATUS.WAITING);
            addToQueue(j);
        }
        setDependencies(aJob, dependencies);

        // check if it is a wrappedJob
        if(this.getId().equals(aJob.getId())) {
            WrappedJob = aJob;
        }
    }

    /**
     * @return the thread state
     */
    public int getState() {
        return this.runnerState;
    }

    private void checkRunningJobs(){
        Map<String, Job> oldJobs = null;
        oldJobs = this.runningJobs;
        this.runningJobs = new Hashtable<String, Job>();

        for(Job nextJob: oldJobs.values()) {
            // next job will update its Status in run (once it is finished)
            this.addToQueue(nextJob);
        }
    }

    private void checkWaitingJobs() {
        Map<String, Job> oldJobs = null;
        oldJobs = this.waitingJobs;
        this.waitingJobs = new Hashtable<String, Job>();

        for(Job nextJob: oldJobs.values()) {
            // logger.info ("Waiting() Job:" + nextJob.getId());
            synchronized(_jobStatus) {
                if(JobSTATUS.WAITING == getJobStatus(nextJob)) {
                    JobSTATUS newStatus = this.updateWaitingState(nextJob);
                    setJobStatus(nextJob, newStatus);
                    this.addToQueue(nextJob);
                } else {
                    if(JobSTATUS.READY == getJobStatus(nextJob)
                       || JobSTATUS.RUNNING == getJobStatus(nextJob)) {
                        // Forcefully add to waiting list.
                        this.waitingJobs.put(nextJob.getId(), nextJob);
                    } else {
                        this.addToQueue(nextJob);
                    }
                }
            }
        }
    }

    private void startReadyJobs(){
        Map<String, Job> oldJobs = null;
        oldJobs = this.readyJobs;
        this.readyJobs = new Hashtable<String, Job>();

        for(final Job nextJob: oldJobs.values()) {
            // Do not start if already started.
            boolean shouldRun = false;
            synchronized(_jobStatus) {
                if(getJobStatus(nextJob).equals(JobSTATUS.READY)) {
                    setJobStatus(nextJob, JobSTATUS.RUNNING);
                    shouldRun = true;
                }
            }

            if(shouldRun) {
                info("Starting sub-job '" + nextJob.getId() + "'");
                _threadPool.execute(new Runnable() {

                    public void run() {
                        if(_descriptors != null) {

                            JobDescriptor desc = _descriptors.get(nextJob.getId());
                            if(desc == null)
                                throw new IllegalStateException("Attempt to run graph sub-job "
                                                                + nextJob.getId()
                                                                + " but no descriptor was found for this job.");

                            Thread.currentThread().setContextClassLoader(desc.getClassLoader());
                        }

                        try {
                            nextJob.run();
                            setJobStatus(nextJob, JobSTATUS.SUCCESS);
                        } catch(Throwable t) {
                            Logger.getLogger(nextJob.getId())
                                  .error("Error in job '" + nextJob.getId() + "'", t);
                            t.printStackTrace();
                            setJobStatus(nextJob, JobSTATUS.FAILED);
                            // don't swallow fatal errors
                            if(t instanceof Error)
                                throw (Error) t;
                        }
                        info("Job '" + nextJob.getId() + "' is complete.");
                    }
                });
            }
            // update state in all cases
            this.addToQueue(nextJob);
        }
    }

    public boolean allFinished() {
        return this.waitingJobs.size() == 0 && this.readyJobs.size() == 0
               && this.runningJobs.size() == 0;
    }

    private JobSTATUS updateWaitingState(Job job) {
        JobSTATUS tempStatus = JobSTATUS.READY;
        // logger.info ("UpdateWaiting() Job:" + job.getId());

        if(getDependencies(job).size() != 0) {
            for(Job pred: getDependencies(job)) {
                // logger.info ("checking waitStatus for Job:" + pred.getId());
                JobSTATUS s = getJobStatus(pred);

                switch(s) {
                    // Mark current Job Dependent Failed if anyone of the parent
                    // job fails
                    case FAILED:
                        tempStatus = JobSTATUS.FAILED;
                        error("Failing as dependent job" + pred.getId() + " failed");
                        _exceptionString += "Job:" + job.getId() + " Failing as Dependent Job "
                                            + pred.getId() + " failed\n";
                        break;

                    case DEPENDENT_FAILED:
                        error("Failing as Dependent Job" + pred.getId() + " failed");
                        tempStatus = JobSTATUS.DEPENDENT_FAILED;
                        _exceptionString += "Job:" + job.getId() + " Failing as Dependent Job "
                                            + pred.getId() + " failed\n";
                        break;

                    // Mark current job waiting state if any of the parent is
                    // not yet finished.
                    case READY: /* Intentional fallover to Waiting */
                    case RUNNING: /* Intentional fallover to Waiting */
                    case WAITING:
                        tempStatus = JobSTATUS.WAITING;
                        break;

                    case SUCCESS: // Do nothing tempStatus is initialized as
                                  // READY.
                }
            }
        }
        return tempStatus;
    }

    /**
     * The main loop for the thread. The loop does the following: Check the
     * states of the running jobs Update the states of waiting jobs Submit the
     * jobs in ready state
     */
    public void run() {
        this.runnerState = JobGraph.RUNNING;
        while(true) {
            if(this.runnerState == JobGraph.STOPPING) {
                // clear waiting/ready Jobs
                waitingJobs.clear();
                readyJobs.clear();

                // Kill all running Jobs
                for(Job job: runningJobs.values()) {
                    try {
                        job.cancel();
                    } catch(Exception e) {
                        throw new RuntimeException("Error while cancelling Job:" + job.getId(), e);
                    }
                }
            }

            // printStats();
            checkRunningJobs();
            checkWaitingJobs();
            startReadyJobs();

            // Lets check if all jobs are finished here
            if(waitingJobs.size() == 0 && runningJobs.size() == 0 && readyJobs.size() == 0) {
                this.runnerState = JobGraph.STOPPED;
            }

            if(this.runnerState == JobGraph.STOPPED) {
                break;
            }

            try {
                Thread.sleep(500);
            } catch(Exception e) {

            }
        }

        if(null != WrappedJob) {
            if(JobSTATUS.SUCCESS != getJobStatus(WrappedJob)) {
                throw new RuntimeException("GraphJob(" + this.getId() + ")  Failed with Exception message:\n" + _exceptionString);
            }
        }
    }

    public String printSet(Set<String> set) {
        StringBuilder str = new StringBuilder();
        for(String s: set) {
            str.append(s).append("|");
        }
        return str.toString();
    }

    public String printEntrySet(Set<Entry<Job, JobSTATUS>> set) {
        StringBuilder str = new StringBuilder();
        for(Entry<Job, JobSTATUS> entry: set) {
            str.append(entry.getKey().getId()).append(":").append(entry.getValue()).append("|");
        }
        return str.toString();
    }

    public void cancel() throws Exception {
        this.runnerState = JobGraph.STOPPING;
    }
}
