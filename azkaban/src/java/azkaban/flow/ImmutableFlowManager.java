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

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import azkaban.serialization.FlowExecutionSerializer;
import azkaban.serialization.de.FlowExecutionDeserializer;
import org.apache.commons.fileupload.util.Streams;
import org.apache.commons.io.IOUtils;
import org.joda.time.DateTime;
import org.json.JSONObject;

import azkaban.common.utils.Props;
import azkaban.serialization.ExecutableFlowSerializer;
import azkaban.serialization.de.ExecutableFlowDeserializer;
import azkaban.util.JSONToJava;

/**
 * An "append-only" set of Flows.  If you need to remove flows, this object should be thrown away and a
 * new one built to replace it.
 */
public class ImmutableFlowManager implements FlowManager
{
    private final JSONToJava jsonToJava;
    private final Map<String, Flow> flowsMap;
    private final Set<String> rootFlowNames;
    private final AtomicLong lastId;

    private final Map<String, List<String>> folderToRoot;
    
    private final File storageDirectory;
    private final FlowExecutionSerializer serializer;
    private final FlowExecutionDeserializer deserializer;

    public ImmutableFlowManager(
            Map<String, Flow> flowMap,
            Set<String> rootFlows,
            Map<String, List<String>> folderToRoot, 
            FlowExecutionSerializer serializer,
            FlowExecutionDeserializer deserializer,
            File storageDirectory,
            long lastId
    )
    {
    	this.folderToRoot = folderToRoot;
        this.flowsMap = flowMap;
        this.rootFlowNames = rootFlows;
        this.serializer = serializer;
        this.deserializer = deserializer;
        this.storageDirectory = storageDirectory;

        this.lastId = new AtomicLong(lastId);
        this.jsonToJava = new JSONToJava();
    }

    
    @Override
    public boolean hasFlow(String name)
    {
        return flowsMap.containsKey(name);
    }
    
    @Override
    public Flow getFlow(String name)
    {
        return flowsMap.get(name);
    }

    @Override
    public Collection<Flow> getFlows()
    {
        return flowsMap.values();
    }

    @Override
    public Set<String> getRootFlowNames()
    {
        return Collections.unmodifiableSet(rootFlowNames);
    }

    @Override
    public Iterator<Flow> iterator()
    {
        return getFlows().iterator();
    }

    @Override
    public ExecutableFlow createNewExecutableFlow(String name)
    {
        final Flow flow = getFlow(name);

        if (flow == null) {
            return null;
        }
        
        String flowId = String.valueOf(getNextId());
        
        return flow.createExecutableFlow(flowId, new HashMap<String, ExecutableFlow>());
    }

    @Override
    public long getNextId()
    {
        return lastId.incrementAndGet();
    }

    @Override
    public long getCurrMaxId()
    {
        return lastId.get();
    }

    @Override
    public FlowExecutionHolder saveExecutableFlow(FlowExecutionHolder holder)
    {
        File storageFile = new File(storageDirectory, String.format("%s.json", holder.getFlow().getId()));

        JSONObject jsonObj = new JSONObject(serializer.apply(holder));

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

        return holder;
    }

    @Override
    public FlowExecutionHolder loadExecutableFlow(long id)
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

    @Override
    public void reload()
    {
        throw new UnsupportedOperationException();
    }

	@Override
	public List<String> getFolders() {
		ArrayList<String> folders = new ArrayList<String>(folderToRoot.keySet());
		Collections.sort(folders);
		return folders;
	}

	@Override
	public List<String> getRootNamesByFolder(String folder) {
		return folderToRoot.get(folder);
	}
}
