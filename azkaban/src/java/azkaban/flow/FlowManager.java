package azkaban.flow;

import azkaban.app.JobDescriptor;
import azkaban.app.JobManager;
import azkaban.serialization.ExecutableFlowSerializer;
import azkaban.serialization.de.ExecutableFlowDeserializer;
import azkaban.util.JSONToJava;
import org.apache.commons.fileupload.util.Streams;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An "append-only" set of Flows.  If you need to remove flows, this object should be thrown away and a
 * new one built to replace it.
 */
public class FlowManager implements Iterable<Flow>
{
    private final JSONToJava jsonToJava;
    private final Map<String, Flow> flowsMap;
    private final AtomicLong lastId;

    private final File storageDirectory;
    private final ExecutableFlowSerializer serializer;
    private final ExecutableFlowDeserializer deserializer;
    private final Set<String> rootFlowNames;

    public FlowManager(
            ExecutableFlowSerializer serializer,
            ExecutableFlowDeserializer deserializer,
            File storageDirectory,
            long lastId
    )
    {
        this.serializer = serializer;
        this.deserializer = deserializer;
        this.storageDirectory = storageDirectory;

        this.lastId = new AtomicLong(lastId);
        this.flowsMap = new ConcurrentSkipListMap<String, Flow>();
        this.jsonToJava = new JSONToJava();

        rootFlowNames = new ConcurrentSkipListSet<String>();
    }

    public void registerFlow(Flow flow)
    {
        if (flow == null) {
            throw new IllegalArgumentException("flow cannot be null");
        }

        flowsMap.put(flow.getName(), flow);
    }

    public boolean hasFlow(String name)
    {
        return flowsMap.containsKey(name);
    }
    
    public Flow getFlow(String name)
    {
        return flowsMap.get(name);
    }

    public Collection<Flow> getFlows()
    {
        return flowsMap.values();
    }

    public void addRootFlowName(String name)
    {
        rootFlowNames.add(name);
    }

    public Set<String> getRootFlowNames()
    {
        return Collections.unmodifiableSet(rootFlowNames);
    }

    @Override
    public Iterator<Flow> iterator()
    {
        return getFlows().iterator();
    }

    public ExecutableFlow createNewExecutableFlow(String name)
    {
        return getFlow(name).createExecutableFlow(String.valueOf(getNextId()), new HashMap<String, ExecutableFlow>());
    }

    public long getNextId()
    {
        return lastId.incrementAndGet();
    }

    public long getCurrMaxId()
    {
        return lastId.get();
    }

    public ExecutableFlow saveExecutableFlow(ExecutableFlow flow)
    {
        File storageFile = new File(storageDirectory, String.format("%s.json", flow.getId()));

        Map<String, Object> map = serializer.apply(flow);
        JSONObject jsonObj = new JSONObject(map);

        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new FileWriter(storageFile));
            out.write(jsonObj.toString(2));
            out.flush();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        finally {
            IOUtils.closeQuietly(out);
        }

        return flow;
    }

    public ExecutableFlow loadExecutableFlow(long id)
    {
        File storageFile = new File(storageDirectory, String.format("%s.json", id));

        if (! storageFile.exists()) {
            return null;
        }

        BufferedInputStream in = null;
        try {
            in = new BufferedInputStream(new FileInputStream(storageFile));

            JSONObject jsonObj = new JSONObject(Streams.asString(in));

            return deserializer.apply(jsonToJava.apply(jsonObj));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        finally {
            IOUtils.closeQuietly(in);
        }
    }
}
