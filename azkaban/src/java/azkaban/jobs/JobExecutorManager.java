package azkaban.jobs;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import azkaban.app.JobDescriptor;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.format.PeriodFormat;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import azkaban.app.AppCommon;
import azkaban.app.JobManager;
import azkaban.app.Mailman;
import azkaban.common.utils.Props;
import azkaban.common.utils.Utils;
import azkaban.flow.ExecutableFlow;
import azkaban.flow.FlowCallback;
import azkaban.flow.FlowExecutionHolder;
import azkaban.flow.FlowManager;
import azkaban.monitor.MonitorImpl;
import azkaban.monitor.MonitorInterface.WorkflowState;
import azkaban.monitor.MonitorInternalInterface.WorkflowAction;
import azkaban.util.process.ProcessFailureException;

public class JobExecutorManager {
    private static Logger logger = Logger.getLogger(JobExecutorManager.class);
    private final Mailman mailman;
    private final JobManager jobManager;
    private final String jobSuccessEmail;
    private final String jobFailureEmail;
    private Properties runtimeProps = null;
    private final FlowManager allKnownFlows;
    private final ThreadPoolExecutor executor;
    private final Map<String, ExecutingJobAndInstance> executing;
    private final Multimap<String, JobExecution> completed;
    
    @SuppressWarnings("unchecked")
	public JobExecutorManager(
    		FlowManager allKnownFlows,
    		JobManager jobManager,
    		Mailman mailman,                     
    		String jobSuccessEmail,
            String jobFailureEmail,
            int maxThreads
    ) {
    	this.jobManager = jobManager;
    	this.mailman = mailman;
    	this.jobSuccessEmail = jobSuccessEmail;
    	this.jobFailureEmail = jobFailureEmail;
    	this.allKnownFlows = allKnownFlows;
        Multimap<String, JobExecution> typedMultiMap = HashMultimap.create();
    	
        this.completed = Multimaps.synchronizedMultimap(typedMultiMap);
    	this.executing = new ConcurrentHashMap<String, ExecutingJobAndInstance>();
    	this.executor = new ThreadPoolExecutor(0, maxThreads, 10, TimeUnit.SECONDS, new LinkedBlockingQueue(), new ExecutorThreadFactory());
    }
    
    /**
     * Cancels an already running job.
     * 
     * @param name
     * @throws Exception
     */
    public void cancel(String name) throws Exception {
    	ExecutingJobAndInstance instance = executing.get(name);
        if(instance == null) {
            throw new IllegalArgumentException("'" + name + "' is not currently running.");
        }
        
        instance.getExecutableFlow().cancel();
    }

    /**
     * Run job file given the id
     * 
     * @param id
     * @param ignoreDep
     */
    public void execute(String id, boolean ignoreDep) {
    	final ExecutableFlow flowToRun = allKnownFlows.createNewExecutableFlow(id);

    	if (isExecuting(id)) {
    		throw new JobExecutionException("Job " + id + " is already running.");
    	}
    	
        if(ignoreDep) {
            for(ExecutableFlow subFlow: flowToRun.getChildren()) {
                subFlow.markCompleted();
            }
        }
        
        execute(flowToRun);
    }
    
    /**
     * Runs the job immediately
     * 
     * @param holder The execution of the flow to run
     */
    public void execute(ExecutableFlow flow) {
    	if (isExecuting(flow.getName())) {
    		throw new JobExecutionException("Job " + flow.getName() + " is already running.");
    	}
    	
        final Props parentProps = produceParentProperties(flow);
        FlowExecutionHolder holder = new FlowExecutionHolder(flow, parentProps);
        logger.info("Executing job '" + flow.getName() + "' now");

        final JobExecution executingJob = new JobExecution(flow.getName(),
                                                       new DateTime(),
                                                       true);
        MonitorImpl.getInternalMonitorInterface().workflowEvent(flow.getId(),
                System.currentTimeMillis(),
                WorkflowAction.START_WORKFLOW,
                WorkflowState.NOP,
                flow.getName());

        executor.execute(new ExecutingFlowRunnable(holder, executingJob));
    }
   
    /**
     * Schedule this flow to run one time at the specified date
     * 
     * @param holder The execution of the flow to run
     */
    public void execute(FlowExecutionHolder holder) {
        ExecutableFlow flow = holder.getFlow();
        
    	if (isExecuting(flow.getName())) {
    		throw new JobExecutionException("Job " + flow.getName() + " is already running.");
    	}
        
        logger.info("Executing job '" + flow.getName() + "' now");

        final JobExecution executingJob = new JobExecution(flow.getName(),
                                                       new DateTime(),
                                                       true);
        executor.execute(new ExecutingFlowRunnable(holder, executingJob));
    }
    
    /**
     * set runtime properties
     * 
     * @param p
     */
    public void setRuntimeProperties(Properties p) {
        runtimeProps = p;
    }

    /**
     * get runtime property
     * 
     * @param name property name
     * @return property value
     */
    public String getRuntimeProperty(String name) {
        return (runtimeProps == null) ? null : runtimeProps.getProperty(name);
    }

    /**
     * set runtime property
     * 
     * @param name property name
     * @param value property value
     */
    public void setRuntimeProperty(String name, String value) {
        if(runtimeProps == null) {
            runtimeProps = new Properties();
        }
        runtimeProps.setProperty(name, value);
    }

    
    /*
     * Wrap a single exception with the name of the scheduled job
     */
    private void sendErrorEmail(JobExecution job,
                                Throwable exception,
                                String senderAddress,
                                List<String> emailList) {
        Map<String, Throwable> map = new HashMap<String, Throwable>();
        map.put(job.getId(), exception);
        sendErrorEmail(job, map, senderAddress, emailList);
    }
    

    /*
     * Send error email
     * 
     * @param job scheduled job
     * 
     * @param exceptions exceptions thrown by failed jobs
     * 
     * @param senderAddress email address of sender
     * 
     * @param emailList email addresses of receivers
     */
    private void sendErrorEmail(JobExecution job,
                                Map<String, Throwable> exceptions,
                                String senderAddress,
                                List<String> emailList) {
        if((emailList == null || emailList.isEmpty()) && jobFailureEmail != null)
            emailList = Arrays.asList(jobFailureEmail);

        if(emailList != null && mailman != null) {
            try {

                StringBuffer body = new StringBuffer("The job '"
                                                     + job.getId()
                                                     + "' running on "
                                                     + InetAddress.getLocalHost().getHostName()
                                                     + " has failed with the following errors: \r\n\r\n");
                int errorNo = 1;
                String logUrlPrefix = runtimeProps != null ? runtimeProps.getProperty(AppCommon.DEFAULT_LOG_URL_PREFIX)
                                                           : null;
                if(logUrlPrefix == null && runtimeProps != null) {
                    logUrlPrefix = runtimeProps.getProperty(AppCommon.LOG_URL_PREFIX);
                }

                final int lastLogLineNum = 60;
                for(Map.Entry<String, Throwable> entry: exceptions.entrySet()) {
                    final String jobId = entry.getKey();
                    final Throwable exception = entry.getValue();

                    /* append job exception */
                    String error = (exception instanceof ProcessFailureException) ? ((ProcessFailureException) exception).getLogSnippet()
                                                                                 : Utils.stackTrace(exception);
                    body.append(" Job " + errorNo + ". " + jobId + ":\n" + error + "\n");

                    /* append log file link */
                    JobExecution jobExec = jobManager.loadMostRecentJobExecution(jobId);
                    if(jobExec == null) {
                        body.append("Job execution object is null for jobId:" + jobId + "\n\n");
                    }

                    String logPath = jobExec != null ? jobExec.getLog() : null;
                    if(logPath == null) {
                        body.append("Log path is null. \n\n");
                    } else {
                        body.append("See log in " + logUrlPrefix + logPath + "\n\n" + "The last "
                                    + lastLogLineNum + " lines in the log are:\n");

                        /* append last N lines of the log file */
                        String logFilePath = this.jobManager.getLogDir() + File.separator
                                             + logPath;
                        Vector<String> lastNLines = Utils.tail(logFilePath, 60);

                        if(lastNLines != null) {
                            for(String line: lastNLines) {
                                body.append(line + "\n");
                            }
                        }
                    }

                    errorNo++;
                }

                // logger.error("\n\n error email body: \n" + body.toString() +
                // "\n");

                mailman.sendEmailIfPossible(senderAddress,
                                             emailList,
                                             "Job '" + job.getId() + "' has failed!",
                                             body.toString());

            } catch(UnknownHostException uhe) {
                logger.error(uhe);
            }
        }
    }

    private void sendSuccessEmail(JobExecution job,
                                  Duration duration,
                                  String senderAddress,
                                  List<String> emailList) {
        if((emailList == null || emailList.isEmpty()) && jobSuccessEmail != null) {
            emailList = Arrays.asList(jobSuccessEmail);
        }

        if(emailList != null && mailman != null) {
            try {
                mailman.sendEmailIfPossible(senderAddress,
                                             emailList,
                                             "Job '" + job.getId() + "' has completed on "
                                                     + InetAddress.getLocalHost().getHostName()
                                                     + "!",
                                             "The job '"
                                                     + job.getId()
                                                     + "' completed in "
                                                     + PeriodFormat.getDefault()
                                                                   .print(duration.toPeriod())
                                                     + ".");
            } catch(UnknownHostException uhe) {
                logger.error(uhe);
            }
        }
    }
    
    private Props produceParentProperties(final ExecutableFlow flow) {
        Props parentProps = new Props();

        parentProps.put("azkaban.flow.id", flow.getId());
        parentProps.put("azkaban.flow.uuid", UUID.randomUUID().toString());

        DateTime loadTime = new DateTime();

        parentProps.put("azkaban.flow.start.timestamp", loadTime.toString());
        parentProps.put("azkaban.flow.start.year", loadTime.toString("yyyy"));
        parentProps.put("azkaban.flow.start.month", loadTime.toString("MM"));
        parentProps.put("azkaban.flow.start.day", loadTime.toString("dd"));
        parentProps.put("azkaban.flow.start.hour", loadTime.toString("HH"));
        parentProps.put("azkaban.flow.start.minute", loadTime.toString("mm"));
        parentProps.put("azkaban.flow.start.seconds", loadTime.toString("ss"));
        parentProps.put("azkaban.flow.start.milliseconds", loadTime.toString("SSS"));
        parentProps.put("azkaban.flow.start.timezone", loadTime.toString("ZZZZ"));
        return parentProps;
    }
    
    /**
     * A thread factory that sets the correct classloader for the thread
     */
    public class ExecutorThreadFactory implements ThreadFactory {

        private final AtomicInteger threadCount = new AtomicInteger(0);

        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("scheduler-thread-" + threadCount.getAndIncrement());
            return t;
        }
    }
    
    public class ExecutingJobAndInstance {

        private final ExecutableFlow flow;
        private final JobExecution scheduledJob;

        private ExecutingJobAndInstance(ExecutableFlow flow, JobExecution scheduledJob) {
            this.flow = flow;
            this.scheduledJob = scheduledJob;
        }

        public ExecutableFlow getExecutableFlow() {
            return flow;
        }

        public JobExecution getScheduledJob() {
            return scheduledJob;
        }
    }
   
    /**
     * A runnable adapter for a Job
     */
    private class ExecutingFlowRunnable implements Runnable {

        private final JobExecution runningJob;
        private final FlowExecutionHolder holder;

        private ExecutingFlowRunnable(FlowExecutionHolder holder, JobExecution runningJob) {
            this.holder = holder;
            this.runningJob = runningJob;
        }

        public void run() {
            final ExecutableFlow flow = holder.getFlow();
            logger.info("Starting run of " + flow.getName());

            List<String> emailList = null;
            String senderAddress = null;
            try {
                final JobDescriptor jobDescriptor = jobManager.getJobDescriptor(flow.getName());

                emailList = jobDescriptor.getEmailNotificationList();
                final List<String> finalEmailList = emailList;

                senderAddress = jobDescriptor.getSenderEmail();
                final String senderEmail = senderAddress;

                // mark the job as executing
                runningJob.setStartTime(new DateTime());

                executing.put(flow.getName(), new ExecutingJobAndInstance(flow, runningJob));
                flow.execute(holder.getParentProps(), new FlowCallback() {

                    @Override
                    public void progressMade() {
                        allKnownFlows.saveExecutableFlow(holder);
                    }

                    @Override
                    public void completed(Status status) {
                    	runningJob.setEndTime(new DateTime());

                    	MonitorImpl.getInternalMonitorInterface().workflowEvent(flow.getId(), 
                    	        System.currentTimeMillis(),
                    	        WorkflowAction.END_WORKFLOW, 
                    	        (status == Status.SUCCEEDED ?  WorkflowState.SUCCESSFUL : 
                    	        (status == Status.FAILED ? WorkflowState.FAILED : WorkflowState.UNKNOWN)),
                    	        flow.getName());

                        try {
                            allKnownFlows.saveExecutableFlow(holder);
                            switch(status) {
                                case SUCCEEDED:
                                    if (jobDescriptor.getSendSuccessEmail()) {
                                        sendSuccessEmail(
                                                runningJob,
                                                runningJob.getExecutionDuration(),
                                                senderEmail,
                                                finalEmailList
                                        );
                                    }
                                    break;
                                case FAILED:
                                    sendErrorEmail(runningJob,
                                                   flow.getExceptions(),
                                                   senderEmail,
                                                   finalEmailList);
                                    break;
                                default:
                                    sendErrorEmail(runningJob,
                                                   new RuntimeException(String.format("Got an unknown status[%s]",
                                                                                      status)),
                                                   senderEmail,
                                                   finalEmailList);
                            }
                        } catch(RuntimeException e) {
                            logger.warn("Exception caught while saving flow/sending emails", e);
                            executing.remove(runningJob.getId());
                            throw e;
                        } finally {
                            // mark the job as completed
                            executing.remove(runningJob.getId());
                            completed.put(runningJob.getId(), runningJob);
                        }
                    }
                });

                allKnownFlows.saveExecutableFlow(holder);
            } catch(Throwable t) {
            	executing.remove(runningJob.getId());
            	if(emailList != null) {
                    sendErrorEmail(runningJob, t, senderAddress, emailList);
                }
                
                logger.warn(String.format("An exception almost made it back to the ScheduledThreadPool from job[%s]",
                		runningJob),
                            t);
            }
        }
    }

    public boolean isExecuting(String name) {
        return executing.containsKey(name);
    }

    public Collection<ExecutingJobAndInstance> getExecutingJobs() {
        return executing.values();
    }

    public Multimap<String, JobExecution> getCompleted() {
        return completed;
    }
}