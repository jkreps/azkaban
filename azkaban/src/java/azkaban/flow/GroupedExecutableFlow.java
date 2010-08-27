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

import azkaban.common.utils.Props;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class GroupedExecutableFlow implements ExecutableFlow
{
    private final Object sync = new Object();
    private final String id;
    private final ExecutableFlow[] flows;
    private final ExecutableFlow[] sortedFlows;

    private volatile Status jobState;
    private volatile List<FlowCallback> callbacksToCall;
    private volatile DateTime startTime;
    private volatile DateTime endTime;
    private volatile GroupedExecutableFlow.GroupedFlowCallback theGroupCallback;
    private volatile Throwable exception;
    private volatile Props parentProps;
    private volatile Props returnProps;

    public GroupedExecutableFlow(String id, ExecutableFlow... flows)
    {
        this.id = id;
        this.flows = flows;
        this.sortedFlows = Arrays.copyOf(this.flows, this.flows.length);
        Arrays.sort(this.sortedFlows, new Comparator<ExecutableFlow>()
        {
            @Override
            public int compare(ExecutableFlow o1, ExecutableFlow o2)
            {
                return o1.getName().compareTo(o2.getName());
            }
        });

        jobState = Status.READY;
        updateState();
        callbacksToCall = new ArrayList<FlowCallback>();

        theGroupCallback = new GroupedFlowCallback();

        switch (jobState) {
            case SUCCEEDED:
            case COMPLETED:
            case FAILED:
                DateTime theStartTime = new DateTime();
                DateTime theEndTime = new DateTime(0);
                for (ExecutableFlow flow : flows) {
                    final DateTime subFlowStartTime = flow.getStartTime();
                    if (theStartTime.isAfter(subFlowStartTime)) {
                        theStartTime = subFlowStartTime;
                    }

                    final DateTime subFlowEndTime = flow.getEndTime();
                    if (subFlowEndTime != null && subFlowEndTime.isAfter(theEndTime)) {
                        theEndTime = subFlowEndTime;
                    }
                }

                setAndVerifyParentProps();
                startTime = theStartTime;
                endTime = theEndTime;
                break;
            default:
                // Check for Flows that are "RUNNING"
                boolean allRunning = true;
                List<ExecutableFlow> runningFlows = new ArrayList<ExecutableFlow>();
                DateTime thisStartTime = null;

                for (ExecutableFlow flow : flows) {
                    if (flow.getStatus() != Status.RUNNING) {
                        allRunning = false;

                        final DateTime subFlowStartTime = flow.getStartTime();
                        if (subFlowStartTime != null && subFlowStartTime.isBefore(thisStartTime)) {
                            thisStartTime = subFlowStartTime;
                        }
                    }
                    else {
                        runningFlows.add(flow);
                    }
                }

                if (allRunning) {
                    jobState = Status.RUNNING;
                }

                for (ExecutableFlow runningFlow : runningFlows) {
                    final DateTime subFlowStartTime = runningFlow.getStartTime();
                    if (subFlowStartTime != null && subFlowStartTime.isBefore(thisStartTime)) {
                        thisStartTime = subFlowStartTime;
                    }
                }
                setAndVerifyParentProps();

                startTime = thisStartTime;
                endTime = null;

                // Make sure everything is initialized before leaking the pointer to "this".
                // This is just installing the callback in an already running flow.
                for (ExecutableFlow runningFlow : runningFlows) {
                    runningFlow.execute(parentProps, theGroupCallback);
                }
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
        return StringUtils.join(
                Iterables.transform(Arrays.asList(flows), new Function<ExecutableFlow, String>()
                {
                    @Override
                    public String apply(ExecutableFlow flow)
                    {
                        return flow.getName();
                    }
                }).iterator(),
                " + "
        );
    }
    
    @Override
    public Props getReturnProps() {
        return returnProps;
    }

    @Override
    public void execute(Props parentProps, final FlowCallback callback)
    {
        if (parentProps == null) {
            parentProps = new Props();
        }

        synchronized (sync) {
            if (this.parentProps == null) {
                this.parentProps = parentProps;
            }
            else if (jobState != Status.COMPLETED && !this.parentProps.equalsProps(parentProps)) {
                throw new IllegalArgumentException(
                        String.format(
                                "%s.execute() called with multiple differing parentProps objects.  " +
                                "Call reset() before executing again with a different Props object. this.parentProps[%s], parentProps[%s]",
                                getClass().getSimpleName(),
                                this.parentProps,
                                parentProps
                        )
                );
            }

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
                    return;
            }
        }

        if (startTime == null) {
            startTime = new DateTime();
        }

        for (ExecutableFlow flow : flows) {
            if (jobState != Status.FAILED) {
                try {
                    flow.execute(this.parentProps, theGroupCallback);
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
        }
    }

    @Override
    public boolean cancel()
    {
        boolean retVal = true;
        for (ExecutableFlow flow : flows) {
            retVal &= flow.cancel();
        }
        return retVal;
    }

    private void updateState()
    {
        synchronized (sync) {
            boolean allComplete = true;

            for (ExecutableFlow flow : flows) {
                switch (flow.getStatus()) {
                    case FAILED:
                        jobState = Status.FAILED;
                        returnProps = new Props();
                        return;
                    case COMPLETED:
                    case SUCCEEDED:
                        continue;
                    default:
                        allComplete = false;
                }
            }

            if (allComplete) {
                jobState = Status.SUCCEEDED;

                returnProps = new Props();

                for (ExecutableFlow flow : flows) {
                    returnProps = new Props(returnProps, flow.getReturnProps());
                }

                returnProps.logProperties("Output properties for " + getName());
            }
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
                    theGroupCallback = new GroupedFlowCallback();
                    parentProps = null;
                    returnProps = null;
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
                    parentProps = new Props();
                    returnProps = new Props();
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
        return Arrays.asList(sortedFlows);
    }

    @Override
    public String toString()
    {
        return "GroupedExecutableFlow{" +
               "flows=" + (flows == null ? null : Arrays.asList(flows)) +
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
    public Props getParentProps() {
        return parentProps;
    }

    @Override
    public Throwable getException()
    {
        return exception;
    }

    private void callCallbacks(final List<FlowCallback> callbacksList, final Status status)
    {
        if (endTime == null) {
            endTime = new DateTime();
        }

        for (FlowCallback callback : callbacksList) {
            try {
                callback.completed(status);
            }
            catch (RuntimeException t) {
                // TODO: Figure out how to use the logger to log that a callback threw an exception.
            }
        }
    }

    private void setAndVerifyParentProps() {
        for (ExecutableFlow flow : flows) {
            if (flow.getStatus() == Status.READY) {
                continue;
            }

            final Props childsParentProps = flow.getParentProps();
            
            if (parentProps == null) {
                parentProps = childsParentProps;
            }
            else {
                if (childsParentProps != null && !parentProps.equalsProps(childsParentProps)) {
                    throw new IllegalStateException(
                            String.format(
                                    "Parent props differ for sub flows. Flow Id[%s]",
                                    id
                            )
                    );
                }
            }
        }
    }
    
    private class GroupedFlowCallback implements FlowCallback
    {
        private final AtomicBoolean notifiedCallbackAlready;

        public GroupedFlowCallback()
        {
            this.notifiedCallbackAlready = new AtomicBoolean(false);
        }

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
        public void completed(final Status status)
        {
            final List<FlowCallback> callbackList;
            synchronized (sync) {
                updateState();
                callbackList = callbacksToCall; // Get the reference before leaving the synchronized
            }

            if (jobState == Status.SUCCEEDED && notifiedCallbackAlready.compareAndSet(false, true)) {
                callCallbacks(callbackList, Status.SUCCEEDED);
            }
            else if (jobState == Status.FAILED && notifiedCallbackAlready.compareAndSet(false, true)) {
                for (ExecutableFlow flow : flows) {
                    if (exception == null) {
                        exception = flow.getException();
                    }
                }
                callCallbacks(callbackList, Status.FAILED);
            }
            else {
                for (FlowCallback flowCallback : callbackList) {
                    flowCallback.progressMade();
                }
            }
        }
    }
}
