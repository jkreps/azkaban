package azkaban.flow;

import azkaban.app.JobDescriptor;
import azkaban.app.JobManager;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class Flows
{
    public static Flow buildLegacyFlow(
            final JobManager jobManager,
            final FlowManager alreadyBuiltFlows,
            final JobDescriptor rootDescriptor
    )
    {
        //TODO MED: The jobManager isn't really the best Job factory.  It should be revisited, but it works for now.
        if (alreadyBuiltFlows.hasFlow(rootDescriptor.getId())) {
            return alreadyBuiltFlows.getFlow(rootDescriptor.getId());
        }

        final Flow retVal;
        if (rootDescriptor.hasDependencies()) {
            Set<JobDescriptor> dependencies = rootDescriptor.getDependencies();
            List<Flow> dependencyFlows =
                    Lists.newArrayList(
                            Iterables.transform(
                                    dependencies,
                                    new Function<JobDescriptor, Flow>()
                                    {
                                        @Override
                                        public Flow apply(JobDescriptor jobDescriptor)
                                        {
                                            return buildLegacyFlow(jobManager, alreadyBuiltFlows, jobDescriptor);
                                        }
                                    }
                            )
                    );

            retVal = new MultipleDependencyFlow(
                    new IndividualJobFlow(jobManager.loadJob(rootDescriptor.getId(), true)),
                    dependencyFlows.toArray(new Flow[dependencyFlows.size()])
            );
        }
        else {
            retVal = new IndividualJobFlow(jobManager.loadJob(rootDescriptor.getId(), true));
        }

        alreadyBuiltFlows.registerFlow(retVal);

        return retVal;
    }

    public static ExecutableFlow resetFailedFlows(
            final ExecutableFlow theFlow
    )
    {
        if (theFlow.getStatus() == Status.FAILED) {
            theFlow.reset();
        }

        if (theFlow.hasChildren()) {
            for (ExecutableFlow flow : theFlow.getChildren()) {
                resetFailedFlows(flow);
            }
        }

        return theFlow;
    }
}
