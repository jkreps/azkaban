package azkaban.flow;

import org.joda.time.DateTime;

import java.util.List;

/**
 * An ExecutableFlow is a Flow object that has both the graph structure data as well as state required to execute the
 * Flow.  While both an ExecutableFlow and a Flow are immutable from the API's perspective, an instance of an
 * ExecutableFlow represents a single execution of a Flow while an instance of a Flow simply represents the job
 * execution graph.
 */
public interface ExecutableFlow
{
    /**
     * Gets the id for the ExecutableFlow.  This is the same for all individual ExecutableFlow objects within this flow.
     *
     * @return the id for the flow
     */
    public String getId();

    /**
     * Gets the name of this stage of the flow.
     *
     * @return name of this stage of the flow.
     */
    public String getName();

    /**
     * Executes the flow.  This call should not block.  Instead, when execution is complete,
     * it should call the callback() method on the callback.
     *
     * Execute can be called multiple times and, upon completion, should call all callbacks that were passed into
     * an execute() call.  If the job is already completed when an execute() call is made, the callback should still
     * be called.
     *
     * @param callback the callback to be run upon completion.
     */
    public void execute(FlowCallback callback);

    /**
     * Cancels a running flow
     *
     * @return Whether the flow was canceled
     */
    public boolean cancel();

    /**
     * Gets the current state of the flow.
     *
     * @return the current state of the flow.
     */
    public Status getStatus();

    /**
     * Resets the state of the job such that it returns a Status of READY and whatever else that means.
     *
     * @return a boolean value indicating the success of the reset
     */
    public boolean reset();

    /**
     * Sets the state of a job to Status.COMPLETED.  This means that the job is considered "successful" for all intents
     * and purposes, but that it wasn't actually run.
     *
     * @return a boolean value indicating success.  A false return value generally means that adjusting the value at
     * that point could lead to a race condition or some other condition in which the internal state would not
     * be handled properly.
     */
    public boolean markCompleted();

    /**
     * Tells whether or not the Flow has children.  A Flow might have children if it encapsulates a dependency
     * relationship or if it is just grouping up multiple flows into a single unit.
     *
     * @return true if the flow has children
     */
    public boolean hasChildren();

    /**
     * Returns a list of the children Flow objects if any exist.  If there are no children, this should simply
     * return an empty list.
     *
     * @return A list of the children Flow objects.  An empty list if no children.  This should never return null.
     */
    public List<ExecutableFlow> getChildren();

    /**
     * Gets the time the execution started.  Null if not started yet.
     *
     * @return time execution started.  Null if not started yet.
     */
    public DateTime getStartTime();

    /**
     * Gets the time the execution completed.  Null if not started yet.
     *
     * @return time execution completed.  Null if not started yet.
     */
    public DateTime getEndTime();
}
