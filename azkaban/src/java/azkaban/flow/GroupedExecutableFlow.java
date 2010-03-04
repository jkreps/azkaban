package azkaban.flow;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
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
        callbacksToCall = new ArrayList<FlowCallback>();
        startTime = null;
        endTime = null;
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
                    return;
            }
        }

        if (startTime == null) {
            startTime = new DateTime();
        }
        else {
            throw new RuntimeException("Somehow managed to have execute() called with startTime != null");
        }

        final AtomicBoolean notifiedCallbackAlready = new AtomicBoolean(false);
        for (ExecutableFlow flow : flows) {
            if (jobState != Status.FAILED) {
                flow.execute(new FlowCallback()
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
                            callCallbacks(callbackList, Status.FAILED);
                        }
                        else {
                            for (FlowCallback flowCallback : callbackList) {
                                flowCallback.progressMade();
                            }
                        }
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
                });
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
                    startTime = null;
                    endTime = null;
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
}
