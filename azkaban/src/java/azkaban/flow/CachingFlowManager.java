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
import azkaban.flow.ExecutableFlow;
import azkaban.flow.Flow;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * A FlowManager that caches ExecutableFlows.
 *
 * That is, if "createNewExecutableFlow()" generated a Flow with id 2, subsequent calls to loadExecutableFlow(2) would
 * return the same instance.
 */
public class CachingFlowManager implements FlowManager
{
    private static final Logger log = Logger.getLogger(CachingFlowManager.class);

    private final FlowManager baseManager;
    private final Map<String, ExecutableFlow> flowCache;

    public CachingFlowManager(FlowManager baseManager, final int cacheSize)
    {
        this.baseManager = baseManager;

        this.flowCache = Collections.synchronizedMap(
                new LinkedHashMap<String, ExecutableFlow>((int) (cacheSize * 1.5), 0.75f, true){
                    @Override
                    protected boolean removeEldestEntry(Map.Entry<String, ExecutableFlow> eldest)
                    {
                        final boolean tooManyElements = super.size() > cacheSize;

                        if (tooManyElements) {
                            final Status status = eldest.getValue().getStatus();
                            final boolean jobNotRunning = status != Status.RUNNING;

                            if (jobNotRunning) {
                                return true;
                            }
                            else {
                                log.warn(String.format(
                                        "Cache is at size[%s] and should have evicted an entry, but the oldest entry wasn't completed[%s].  Perhaps the cache size is too small",
                                        super.size(),
                                        status
                                ));
                            }
                        }

                        return false;
                    }
                }
        );
    }

    public boolean hasFlow(String name)
    {
        return baseManager.hasFlow(name);
    }

    public Flow getFlow(String name)
    {
        return baseManager.getFlow(name);
    }

    public Collection<Flow> getFlows()
    {
        return baseManager.getFlows();
    }

    public Set<String> getRootFlowNames()
    {
        return baseManager.getRootFlowNames();
    }

    @Override
    public Iterator<Flow> iterator()
    {
        return baseManager.iterator();
    }

    public ExecutableFlow createNewExecutableFlow(String name, Props overrideProps)
    {
        final ExecutableFlow retVal = baseManager.createNewExecutableFlow(name, overrideProps);

        addToCache(retVal);

        return retVal;
    }

    public long getNextId()
    {
        return baseManager.getNextId();
    }

    public long getCurrMaxId()
    {
        return baseManager.getCurrMaxId();
    }

    public ExecutableFlow saveExecutableFlow(ExecutableFlow flow)
    {
        return baseManager.saveExecutableFlow(flow);
    }

    public ExecutableFlow loadExecutableFlow(long id)
    {
        final ExecutableFlow executableFlow = flowCache.get(String.valueOf(id));
        if (executableFlow != null) {
            return executableFlow;
        }

        final ExecutableFlow retVal = baseManager.loadExecutableFlow(id);

        addToCache(retVal);

        return retVal;
    }

    public void reload()
    {
        baseManager.reload();
    }

    private void addToCache(ExecutableFlow retVal)
    {
        if (retVal == null) {
            return;
        }
        
        if (flowCache.put(retVal.getId(), retVal) != null) {
            throw new IllegalStateException(
                    String.format(
                            "Attempted to add object to the cache but the id[%s] already existed.  Flow was [%s]",
                            retVal.getId(),
                            retVal
                    )
            );
        }
    }
}
