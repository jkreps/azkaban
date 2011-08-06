/*
 * Copyright 2010 Adconion, Inc
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
 */package azkaban.monitor.model;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import azkaban.common.jobs.DelegatingJob;
import azkaban.common.jobs.Job;
import azkaban.common.utils.Props;
import azkaban.jobs.AbstractProcessJob;

/**
 *   The point of this class is to track the mappings of job to flowId, and the 
 *   mapping of job to its parent DelegatingJob when that occurs.
 *   This map is used by MonitorImpl and ExecutionModel.
 *
 */
public final class FlowDirectory {
    private static final Logger logger = Logger.getLogger(FlowDirectory.class);
    
    /**
     * Maps job to the flow id of the workflow of which it is part.
     */
    private Map<Job, String> jobToFlowMap = new HashMap<Job, String>(); 
    
    /**
     * Map a job to its parent delegating job parent.  All instances should
     * eventually back-map to a root most job which should have a flow id.
     * Just to refresh, the job model only points forward,  we need this back map
     * at various occasions to get the delegating job parent directly from the inner job.
     */
    private Map<Job, DelegatingJob> delegatingParentMap = new HashMap<Job, DelegatingJob>();
    
    private volatile static FlowDirectory thisDirectory;
    
    private FlowDirectory() { }
    
    /**
     * Accessor to the flow director singleton.
     * @return FlowDirectory
     */
    public static FlowDirectory getFlowDirectory() {
        synchronized (FlowDirectory.class) {
            if (thisDirectory == null) {
                thisDirectory = new FlowDirectory();
            }
        }      
        return thisDirectory;
    }
    
    /**
     * Used in testing to reset the singleton.
     */
    public static void unsetFlowDirectory() {
        synchronized (FlowDirectory.class) {
            thisDirectory = null;
        }
    }
    
    /**
     * Map the job to the flow id. The complexity here comes if job is a delegating job - 
     * we build the back map described earlier from inner job to parent delegating job.
     *   A main challenge here is to get the flowId - which we get here from the inner
     * most job, by way of the 'azkaban.flow.id' property.
     * Note: you should only find delegation depth at max 3.
     * @param job 
     *            Job to decompose and map into constituent jobs 
     * @return String version (int in string representation) of id of the workflow of which
     *         the job is a part.
     */
    public synchronized String mapJob(Job job) {
        Job jobCursor = job;
        while (jobCursor != null) {
            if (jobCursor instanceof DelegatingJob) {
                DelegatingJob parentJob = (DelegatingJob)jobCursor;
                delegatingParentMap.put(parentJob.getInnerJob(), parentJob);
                jobCursor = parentJob.getInnerJob();
            } else {
                // This is the innermost job - this is the gold piece we are looking for.
                // We assume this is a process job which should have 'props' that identifies the
                // flow id.  We will use this for our map.
                if (!(jobCursor instanceof AbstractProcessJob)) {
                    logger.warn("Non-Delegating job that is not a process job: " +
                            jobCursor.getId());
                    return null;
                }
                AbstractProcessJob pJob = (AbstractProcessJob)jobCursor;
                Props props = pJob.getProps();
                if (props == null) {
                    logger.warn("Inner job of delegating job sequence has no props: " + pJob.getId());
                    return null;
                }
                String flowId = props.get("azkaban.flow.id");
                if (flowId == null) {
                    logger.warn("Inner job of delegating job sequence has no flow id: " + pJob.getId());
                    return null;
                }
                
                // Now that we determined the flowId, run through this sequence of jobs again, and
                // map all to the flowId, so we can derive the flowId from a job later.
                mapFlowIdForward(job, flowId);
                return flowId;
            }
        }
        return null;
    }
    
    private void mapFlowIdForward(Job job, String flowId) {
        Job jobCursor = job;
        while (jobCursor != null) {
            if (jobCursor instanceof DelegatingJob) {
                DelegatingJob parentJob = (DelegatingJob)jobCursor;
                jobToFlowMap.put(parentJob, flowId);
                jobCursor = parentJob.getInnerJob();
            } else {
                jobToFlowMap.put(jobCursor, flowId);
                break;
            }
        }
    }
    
    /**
     * Get the delegating job whose inner job is given by the argument.
     * @param job
     *            The job whose delegate parent is requested.
     * @return DelegatingJob parent of the job
     */
    public synchronized DelegatingJob getDelegatingJobParent(Job job) {
        return delegatingParentMap.get(job);
    }
    
    /**
     * Return the workflow id for a given job.
     * @param job
     *            The job of whose workflow id is requested.
     * @return String version of the (int) workflow id of the job.
     */
    public synchronized String getFlowId(Job job) {
        return jobToFlowMap.get(job);
    }
    
    /**
     * Remove a job reference from the FlowDirectory [internal mappings].
     * @param job
     *            the job whose accounting is being removed from the flow directory.
     */
    public synchronized void removeJobReference(Job job) {
        DelegatingJob dJob = delegatingParentMap.get(job);
        if (dJob != null) {
            return;
        }
        
        // If the job is a delegating job [that has finished presumably], we know
        // the inner job is finished - so take the inner job out of both maps.
        // However,
        // if job has a delegating parent, the above will happen when its parent is 
        // completed, so skip.  Otherwise, if job does not have a delegating parent, this is
        // really the end of this job, and we remove its reference to its job flow,
        // as no follow-on call will do that.
        if (job instanceof DelegatingJob) {
            delegatingParentMap.remove(((DelegatingJob)job).getInnerJob());
            jobToFlowMap.remove(((DelegatingJob)job).getInnerJob());
            if (delegatingParentMap.get(job) == null) {
                jobToFlowMap.remove(job);
            }
        } else {   
            jobToFlowMap.remove(job);
        }
    }
}
