package azkaban.flow;

import org.junit.Assert;

import azkaban.jobs.Status;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public abstract class OneCallFlowCallback implements FlowCallback{
    private final AtomicBoolean hasRunOnce;

    public OneCallFlowCallback(AtomicBoolean hasRunOnce)
    {
        this.hasRunOnce = hasRunOnce;
    }

    @Override
    public void progressMade()
    {
    }

    @Override
    public void completed(Status status) {
        if (hasRunOnce.compareAndSet(false, true)) {
            theCallback(status);
        }
        else {
            Assert.fail("Callback was called more than once.");
        }
    }

    protected abstract void theCallback(Status status);
}
