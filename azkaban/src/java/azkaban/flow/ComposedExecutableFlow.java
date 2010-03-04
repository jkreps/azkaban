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
    private volatile List<FlowCallback> callbacksToCall = new ArrayList<FlowCallback>();


    public ComposedExecutableFlow(String id, ExecutableFlow depender, ExecutableFlow dependee)
    {
        this.id = id;
        this.depender = depender;
        this.dependee = dependee;

        jobState = Status.READY;
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
        else {
            throw new RuntimeException("Somehow managed to have execute() called with startTime != null");
        }

        dependee.execute(new FlowCallback()
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

                        depender.execute(new FlowCallback()
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
                                    callbackList = callbacksToCall;
                                }

                                callCallbacks(callbackList, jobState);
                            }
                        });
                        break;
                    case FAILED:
                        synchronized (sync) {
                            jobState = status;
                            callbackList = callbacksToCall;
                        }

                        callCallbacks(callbackList, jobState);
                        break;
                    default:
                        throw new IllegalStateException(String.format("Got unexpected status[%s] back in a callback.", status));
                }
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
        });
    }

    @Override
    public boolean cancel()
    {
        return depender.cancel() && dependee.cancel();
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

    public ExecutableFlow getDepender()
    {
        return depender;
    }

    public ExecutableFlow getDependee()
    {
        return dependee;
    }
}
