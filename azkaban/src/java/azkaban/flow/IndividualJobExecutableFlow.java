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

package azkaban.flow;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import azkaban.app.JobManager;
import azkaban.common.jobs.DelegatingJob;
import azkaban.common.jobs.Job;
import azkaban.common.utils.Props;

/**
 *
 */
public class IndividualJobExecutableFlow implements ExecutableFlow
{
    private static final Logger logger = Logger.getLogger(IndividualJobExecutableFlow.class);
    private static final AtomicLong threadCounter = new AtomicLong(0);

    private final Object sync = new Object();
    private final String id;
    private final String name;
    private final JobManager jobManager;
    private final Props overrideProps;
    
    private Props flowInputGeneratedProperties;

    private volatile Status jobState;
    private volatile List<FlowCallback> callbacksToCall;
    private volatile DateTime startTime;
    private volatile DateTime endTime;
    private volatile Job job;
    private volatile Throwable exception;
    private volatile Props flowOutputGeneratedProperties;

    public IndividualJobExecutableFlow(String id, String name, Props overrideProps, JobManager jobManager)
    {
        this.id = id;
        this.name = name;
        this.jobManager = jobManager;
        this.overrideProps = overrideProps;

        jobState = Status.READY;
        callbacksToCall = new ArrayList<FlowCallback>();
        startTime = null;
        endTime = null;
        exception = null;
    }

    @Override
    public String getId()
    {
        return id;
    }

    @Override
    public String getName()
    {
        return name;
    }
    
    @Override
    public Props getFlowGeneratedProperties() {
        return flowOutputGeneratedProperties;
    }
    
    public Props getOverrideProps() {
        return overrideProps;
    }

    @Override
    public void execute(FlowCallback callback, Props flowGeneratedProperties)
    {
        synchronized (sync) {
            switch (jobState) {
                case READY:
                    jobState = Status.RUNNING;
                    startTime = new DateTime();
                    callbacksToCall.add(callback);
                    break;
                case RUNNING:
                    callbacksToCall.add(callback);
                    return;
                case COMPLETED:
                case SUCCEEDED:
                    callback.completed(Status.SUCCEEDED);
                    return;
                case FAILED:
                    callback.completed(Status.FAILED);
                    return;
            }
        }

        // Get the output properties from dependent jobs.
        // Clone them so we don't mess up storage up the line.
        this.flowInputGeneratedProperties = (flowGeneratedProperties == null ? 
                                                null :
                                                Props.clone(flowGeneratedProperties)); 
        
        try {
            // Only one thread should ever be able to get to this point because of management of jobState
            // Thus, this should only ever get called once before the job finishes (at which point it could be reset)
            job = jobManager.loadJob(getName(), overrideProps, true);
        }
        catch (Exception e) {
            logger.warn(
                    String.format("Exception thrown while creating job[%s]", getName()),
                    e
            );
            job = null;
        }

        if (job == null) {
            logger.warn(
                    String.format("Job[%s] doesn't exist, but was supposed to run.  Perhaps someone changed the flow?", getName())
            );

            final List<FlowCallback> callbackList;

            synchronized (sync) {
                jobState = Status.FAILED;
                callbackList = callbacksToCall; // Get the reference before leaving the synchronized
            }
            callCallbacks(callbackList, jobState);
            return;
        }

        Thread theThread = new Thread(
                new Runnable()
                {
                    @Override
                    public void run()
                    {
                        final List<FlowCallback> callbackList;

                        try {
                            job.run(flowInputGeneratedProperties);
                        }
                        catch (Exception e) {
                            synchronized (sync) {
                                jobState = Status.FAILED;
                                exception = e;
                                callbackList = callbacksToCall; // Get the reference before leaving the synchronized
                            }
                            callCallbacks(callbackList, jobState);

                            throw new RuntimeException(e);
                        }
 
                        synchronized (sync) {
                            jobState = Status.SUCCEEDED;
                            callbackList = callbacksToCall; // Get the reference before leaving the synchronized
                        }
                                          
                        // Retrieve the output properties from the job.
                        // Consolidate what we receive as input with those for output, so we can
                        // Pass the aggregate of properties up the line.
                        // The output should override the input.
                        // Think of the example A-->B-->C and what A would want from B, C
                        Props jobGeneratedProps = new Props();
                        if (flowInputGeneratedProperties != null) {
                            jobGeneratedProps.putAll(flowInputGeneratedProperties);
                        }
                        if (job.getJobGeneratedProperties() != null) {
                            jobGeneratedProps.putAll(job.getJobGeneratedProperties());   
                        }
                        flowOutputGeneratedProperties = jobGeneratedProps;
                        flowOutputGeneratedProperties.logProperties("Output properties for " + name);
                        callCallbacks(callbackList, jobState);
                    }
                },
                String.format("%s thread-%s", job.getId(), threadCounter.getAndIncrement())
        );

        Job currJob = job;
        while (true) {
            if (currJob instanceof DelegatingJob) {
                currJob = ((DelegatingJob) currJob).getInnerJob();
            }
            else {
                break;
            }
        }

        theThread.start();
    }

    @Override
    public boolean cancel()
    {
        final List<FlowCallback> callbacks;
        synchronized (sync) {
            switch(jobState) {
                case COMPLETED:
                case SUCCEEDED:
                case FAILED:
                    return true;
                default:
                    jobState = Status.FAILED;
                    callbacks = callbacksToCall;
                    callbacksToCall = new ArrayList<FlowCallback>();
            }
        }

        for (FlowCallback callback : callbacks) {
            callback.completed(Status.FAILED);
        }

        try {
            if (job != null) {
                job.cancel();
            }

            return true;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Status getStatus()
    {
        return jobState;
    }

    @Override
    public boolean reset()
    {
        synchronized (sync) {
            switch (jobState) {
                case RUNNING:
                    return false;
                default:
                    jobState = Status.READY;
                    callbacksToCall = new ArrayList<FlowCallback>();
                    startTime = null;
                    endTime = null;
                    exception = null;
            }
        }

        return true;
    }

    @Override
    public boolean markCompleted()
    {
        synchronized (sync) {
            switch (jobState) {
                case RUNNING:
                    return false;
                default:
                    jobState = Status.COMPLETED;
            }
        }
        return true;
    }

    @Override
    public boolean hasChildren()
    {
        return false;
    }

    @Override
    public List<ExecutableFlow> getChildren()
    {
        return Collections.emptyList();
    }

    @Override
    public String toString()
    {
        return "IndividualJobExecutableFlow{" +
               "job=" + job +
               ", jobState=" + jobState +
               '}';
    }

    @Override
    public DateTime getStartTime()
    {
        return startTime;
    }

    @Override
    public DateTime getEndTime()
    {
        return endTime;
    }

    @Override
    public Throwable getException()
    {
        return exception;
    }

    IndividualJobExecutableFlow setStatus(Status newStatus)
    {
        synchronized (sync) {
            switch (jobState) {
                case READY:
                    jobState = newStatus;
                    break;
                default:
                    throw new IllegalStateException("Can only set status when job is in the READY state.");
            }
        }

        return this;
    }

    IndividualJobExecutableFlow setStartTime(DateTime startTime)
    {
        this.startTime = startTime;

        return this;
    }

    IndividualJobExecutableFlow setEndTime(DateTime endTime)
    {
        this.endTime = endTime;

        return this;
    }

    private void callCallbacks(final List<FlowCallback> callbackList, final Status status)
    {
        if (endTime == null) {
            endTime = new DateTime();
        }

        Thread callbackThread = new Thread(
                new Runnable()
                {
                    @Override
                    public void run()
                    {
                        for (FlowCallback callback : callbackList) {
                            try {
                                callback.completed(status);
                            }
                            catch (RuntimeException t) {
                                logger.error(
                                        String.format("Exception thrown while calling callback. job[%s]", getName()),
                                        t
                                );
                            }
                        }
                    }
                },
                String.format("%s-callback", Thread.currentThread().getName())
        );

        callbackThread.start();
    }
}
