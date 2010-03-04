package azkaban.flow;

/**
 * A callback to be used when running jobs through a Flow.
 *
 * There is no guarantee about what thread will call a callback.
 */
public interface FlowCallback {

    /**
     * Method called whenever some sub-set of the flow is complete.
     */
    public void progressMade();

    /**
     * Method called when the entire flow has completed and does not have anything else running.
     *
     * @param status the status that the flow ended with.
     */
    public void completed(final Status status);
}
