package azkaban.flow.manager;

import azkaban.app.JobDescriptor;
import azkaban.app.JobManager;
import azkaban.app.JobWrappingFactory;
import azkaban.flow.ExecutableFlow;
import azkaban.flow.Flow;
import azkaban.flow.Flows;
import azkaban.serialization.ExecutableFlowSerializer;
import azkaban.serialization.de.ExecutableFlowDeserializer;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class RefreshableFlowManager implements FlowManager
{
    private final Object idSync = new Object();
    
    private final JobManager jobManager;
    private final JobWrappingFactory jobFactory;
    private final ExecutableFlowSerializer serializer;
    private final ExecutableFlowDeserializer deserializer;
    private final File storageDirectory;

    private final AtomicReference<ImmutableFlowManager> delegateManager;

    public RefreshableFlowManager(
            JobManager jobManager,
            JobWrappingFactory jobFactory,
            ExecutableFlowSerializer serializer,
            ExecutableFlowDeserializer deserializer,
            File storageDirectory,
            long lastId
    )
    {
        this.jobManager = jobManager;
        this.jobFactory = jobFactory;
        this.serializer = serializer;
        this.deserializer = deserializer;
        this.storageDirectory = storageDirectory;

        this.delegateManager = new AtomicReference<ImmutableFlowManager>(null);
        reloadInternal(lastId);
    }

    @Override
    public boolean hasFlow(String name)
    {
        return delegateManager.get().hasFlow(name);
    }

    @Override
    public Flow getFlow(String name)
    {
        return delegateManager.get().getFlow(name);
    }

    @Override
    public Collection<Flow> getFlows()
    {
        return delegateManager.get().getFlows();
    }

    @Override
    public Set<String> getRootFlowNames()
    {
        return delegateManager.get().getRootFlowNames();
    }

    @Override
    public Iterator<Flow> iterator()
    {
        return delegateManager.get().iterator();
    }

    @Override
    public ExecutableFlow createNewExecutableFlow(String name)
    {
        return delegateManager.get().createNewExecutableFlow(name);
    }

    @Override
    public long getNextId()
    {
        synchronized (idSync) {
            return delegateManager.get().getNextId();
        }
    }

    @Override
    public long getCurrMaxId()
    {
        return delegateManager.get().getCurrMaxId();
    }

    @Override
    public ExecutableFlow saveExecutableFlow(ExecutableFlow flow)
    {
        return delegateManager.get().saveExecutableFlow(flow);
    }

    @Override
    public ExecutableFlow loadExecutableFlow(long id)
    {
        return delegateManager.get().loadExecutableFlow(id);
    }

    @Override
    public void reload()
    {
        reloadInternal(null);
    }

    private final void reloadInternal(Long lastId)
    {
        Map<String, Flow> flowMap = new HashMap<String, Flow>();
        Set<String> rootFlows = new TreeSet<String>();
        for (JobDescriptor rootDescriptor : jobManager.getRootJobDescriptors(jobManager.loadJobDescriptors())) {
            if (rootDescriptor.getId() != null) {
                // This call of magical wonderment ends up pushing all Flow objects in the dependency graph for the root into flowMap
                Flows.buildLegacyFlow(jobFactory, jobManager, flowMap, rootDescriptor);
                rootFlows.add(rootDescriptor.getId());
            }
        }

        synchronized (idSync) {
            delegateManager.set(
                    new ImmutableFlowManager(
                            flowMap,
                            rootFlows,
                            serializer,
                            deserializer,
                            storageDirectory,
                            lastId == null ? delegateManager.get().getCurrMaxId() : lastId
                    )
            );
        }
    }
}
