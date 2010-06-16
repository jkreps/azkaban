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

import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class ComposedExecutableFlow implements ExecutableFlow
{

    private final Object sync = new Object();
    private final String id;
    private final ExecutableFlow depender;
    private final ExecutableFlow dependee;

    private volatile DateTime startTime;
    private volatile DateTime endTime;
    private volatile Status jobState;
    private volatile Throwable exception;
    private volatile List<FlowCallback> callbacksToCall = new ArrayList<FlowCallback>();


    public ComposedExecutableFlow(String id, ExecutableFlow depender, ExecutableFlow dependee)
    {
        this.id = id;
        this.depender = depender;
        this.dependee = dependee;

        final Status dependerState = depender.getStatus();
        switch (dependerState) {
            case READY:
                final Status dependeeState = dependee.getStatus();
                switch (dependeeState) {
                    case READY:
                        jobState = Status.READY;
                        startTime = null;
                        endTime = null;
                        break;
                    case RUNNING:
                        jobState = Status.RUNNING;
                        startTime = dependee.getStartTime();
                        endTime = null;
                        dependee.execute(new DependeeCallback());
                        break;
                    case COMPLETED:
                    case SUCCEEDED:
                        jobState = Status.READY;
                        startTime = dependee.getStartTime();
                        endTime = null;
                        break;
                    case FAILED:
                        jobState = Status.FAILED;                        
                        startTime = dependee.getStartTime();
                        endTime = dependee.getEndTime();
                }
                break;
            case RUNNING:
                jobState = Status.RUNNING;
                startTime = dependee.getStartTime() == null ? depender.getStartTime() : dependee.getStartTime();
                endTime = null;

                depender.execute(new DependerCallback());

                break;
            case COMPLETED:
            case SUCCEEDED:
            case FAILED:
                jobState = dependerState;
                startTime = dependee.getStartTime() == null ? depender.getStartTime() : dependee.getStartTime();
                endTime = depender.getEndTime();
        }
    }

    @Override
    public String getId()
    {
        return id;
    }

    @Override
    public String getName()
    {
        return depender.getName();
    }

    @Override
    public void execute(final FlowCallback callback)
    {
        synchronized (sync) {
            switch (jobState) {
                case READY:
                    jobState = Status.RUNNING;
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
                default:
                    return;
            }
        }

        if (startTime == null) {
            startTime = new DateTime();
        }

        try {
            dependee.execute(new DependeeCallback());
        }
        catch (RuntimeException e) {
            final List<FlowCallback> callbacks;
            synchronized (sync) {
                jobState = Status.FAILED;
                callbacks = callbacksToCall;
            }

            callCallbacks(callbacks, Status.FAILED);

            throw e;
        }
    }

    @Override
    public boolean cancel()
    {
        final boolean dependerCanceled = depender.cancel();
        final boolean dependeeCanceled = dependee.cancel();

        return dependerCanceled && dependeeCanceled;
    }

    @Override
    public Status getStatus()
    {
        return jobState;
    }

    @Override
    public boolean reset()
    {
        boolean retVal;

        synchronized (sync) {
            switch (jobState) {
                case RUNNING:
                    return false;
                default:
                    jobState = Status.READY;
                    retVal = depender.reset();
                    callbacksToCall = new ArrayList<FlowCallback>();
                    startTime = null;
                    endTime = null;
                    exception = null;
            }
        }

        return retVal;
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
        return true;
    }

    @Override
    public List<ExecutableFlow> getChildren()
    {
        return Arrays.asList(dependee);
    }

    @Override
    public String toString()
    {
        return "ComposedExecutableFlow{" +
               "depender=" + depender +
               ", dependee=" + dependee +
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

    public ExecutableFlow getDepender()
    {
        return depender;
    }

    public ExecutableFlow getDependee()
    {
        return dependee;
    }

    private void callCallbacks(final List<FlowCallback> callbackList, final Status status)
    {
        if (endTime == null) {
            endTime = new DateTime();
        }

        for (FlowCallback callback : callbackList) {
            try {
                callback.completed(status);
            }
            catch (RuntimeException t) {
                // TODO: Figure out how to use the logger to log that a callback threw an exception.
            }
        }
    }

    private class DependerCallback implements FlowCallback
    {
        @Override
        public void progressMade()
        {
            final List<FlowCallback> callbackList;
            synchronized (sync) {
                callbackList = callbacksToCall;
            }

            for (FlowCallback flowCallback : callbackList) {
                flowCallback.progressMade();
            }
        }

        @Override
        public void completed(Status status)
        {
            final List<FlowCallback> callbackList;

            synchronized (sync) {
                jobState = status;
                if (status == Status.FAILED) {
                    exception = depender.getException();
                }
                callbackList = callbacksToCall;
            }

            callCallbacks(callbackList, status);
        }
    }

    private class DependeeCallback implements FlowCallback
    {
        @Override
        public void progressMade()
        {
            final List<FlowCallback> callbackList;
            synchronized (sync) {
                callbackList = callbacksToCall;
            }

            for (FlowCallback flowCallback : callbackList) {
                flowCallback.progressMade();
            }
        }

        @Override
        public void completed(Status status)
        {
            final List<FlowCallback> callbackList;


            switch (status) {
                case SUCCEEDED:
                    synchronized(sync) {
                        callbackList = callbacksToCall;
                    }
                    for (FlowCallback flowCallback : callbackList) {
                        flowCallback.progressMade();
                    }

                    depender.execute(new DependerCallback());
                    break;
                case FAILED:
                    synchronized (sync) {
                        jobState = status;
                        exception = dependee.getException();
                        callbackList = callbacksToCall;
                    }

                    callCallbacks(callbackList, status);
                    break;
                default:
                    throw new IllegalStateException(String.format("Got unexpected status[%s] back in a callback.", status));
            }
        }
    }
}
