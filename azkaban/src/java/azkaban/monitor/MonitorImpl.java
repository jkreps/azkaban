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
 */package azkaban.monitor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import azkaban.common.jobs.Job;
import azkaban.jobcontrol.impl.jobs.ResourceThrottledJob;
import azkaban.monitor.model.ExecutionModel;
import azkaban.monitor.model.ExecutionModelImpl;
import azkaban.monitor.model.WorkflowExecutionModel;
import azkaban.monitor.stats.ClassStats;
import azkaban.monitor.stats.NativeGlobalStats;
import azkaban.monitor.stats.NativeJobClassStats;
import azkaban.monitor.stats.NativeWorkflowClassStats;

/**
 * Implements both:
 *   1) AzMonitorInterface for clients to use for statistics and notification
 *   2) AzMonitorInternalInterface for fielding events from within Azkaban
 *   
 * This object then implements interfaces for:
 *   1) Running the Azkaban workflow and job events through the execution model.
 *   2) Accessors to global, workflow class, job class statistics
 *   3) Methods for managing the notifications.
 *
 */
public class MonitorImpl implements MonitorInterface, MonitorInternalInterface {
    /**
     * Logger dedicated to this file.
     */
    private static final Logger LOG = Logger.getLogger(MonitorImpl.class);
    
    /**
     * A map over all notifiers to NotificationDetails which tally all response events
     * to which the notifier is to react.
     */
    private Map<MonitorListener, NotificationDetails> notifiers = 
        new HashMap<MonitorListener, NotificationDetails>();
    
    /**
     * Maps workflow root name to list of notifiers that want the workflow class
     * statistics for said workflow root type.  This enumeration shortens the 
     * notification run time.
     */
    private Map<String, List<MonitorListener>> workflowNotifiers = 
        new HashMap<String, List<MonitorListener>>();
    
    /**
     * Maps job name to list of notifiers that want the job class statistics
     * for said job type.   This enumeration shortens the notification run 
     * time.
     */
    private Map<String, List<MonitorListener>> jobNotifiers =
        new HashMap<String, List<MonitorListener>>();
    
    /**
     * The model for all workflow and job instances.
     */
    private ExecutionModel eModel;
    
    /**
     * The monitor is a singleton, with a condition.
     * The condition is that AzkabanApplication constructs the monitor.  It is
     * the right place of control to decide when to construct it.
     * All other accesses are done through the static getMonitor() method.
     * By having the monitor as a singleton, it allows many places in the Azkaban code
     * to get the monitor without laboreously passing the monitor around the code.
     */
    private static MonitorImpl monitor;
    
    /**
     * Constructor
     * Note: This should only be called by AzkabanApplication.
     * A public constructor is a little atypical for a singleton pattern.
     * What is wanted here is for a static accessor 'getMonitor' to be used throughout
     * the Azkaban system.  However, the object construction has to be done at the
     * right place.
     */
    private MonitorImpl() {
        eModel = new ExecutionModelImpl(this);
    }
    
    /**
     * General accessor to the monitor.
     * Note: This should be called first by AzkabanApplication.
     * @return the MonitorImpl
     */
    public static MonitorImpl getMonitor() {
        synchronized (MonitorImpl.class) {
            if (monitor == null) {
                monitor = new MonitorImpl();
            }
        }
        return monitor;
    }
    
    /**
     * Get rid of the monitor; used for testing.
     */
    public static void unsetMonitor() {
        synchronized (MonitorImpl.class) {
            monitor = null;
        }
    }
    
    /**
     * getter.
     * @return the MonitorInterface interface.
     */
    public static MonitorInterface getMonitorInterface() {
        return getMonitor();
    }
    
    /**
     * getter
     * @return the MonitorInternalInterface interface.
     */
    public static MonitorInternalInterface getInternalMonitorInterface() {
            return getMonitor();     
    }
    
    /**************** Global Stats accessors (MonitorInterface) ****************/
    
    /**
     * {@inheritDoc}
     * 
     * @see azkaban.monitor.MonitorInterface#getGlobalAzkabanStats
     *     
     */
    public synchronized NativeGlobalStats getGlobalAzkabanStats() {
        return eModel.getGlobalStatsCopy();
    }
    
    /**
     * {@inheritDoc}
     * 
     * @see azkaban.monitor.MonitorInterface#getWorkflowClassRootJobNames
     *     
     */
    public synchronized List<String> getWorkflowClassRootJobNames() {
        return eModel.getWorkflowClassIds();
    }
    
    /**
     * {@inheritDoc}
     * 
     * @see azkaban.monitor.MonitorInterface#getWorkflowClassStatsByRootJobName
     *     
     */
    public synchronized NativeWorkflowClassStats getWorkflowClassStatsByRootJobName(final String classId) {
        return eModel.getWorkflowClassStatsById(classId);
    }
    
    /**
     * {@inheritDoc}
     * 
     * @see azkaban.monitor.MonitorInterface#getAllWorkflowClassStats
     *     
     */
    public synchronized Map<String, NativeWorkflowClassStats> getAllWorkflowClassStats() {
        return eModel.getAllWorkflowClassStats();
    }
    
    /**
     * {@inheritDoc}
     * 
     * @see azkaban.monitor.MonitorInterface#getJobClassNames
     *     
     */
    public synchronized List<String> getJobClassNames() {
        return eModel.getJobClassIds();
    }
    
    /**
     * {@inheritDoc}
     * 
     * @see azkaban.monitor.MonitorInterface#getJobClassStatsByName
     *     
     */
    public synchronized NativeJobClassStats getJobClassStatsByName(final String jobClassId) {
        return eModel.getJobClassStatsById(jobClassId);
    }
 
    /**
     * {@inheritDoc}
     * 
     * @see azkaban.monitor.MonitorInterface#getAllJobClassStats
     *     
     */
    public synchronized Map<String, NativeJobClassStats> getAllJobClassStats() {
        return eModel.getAllJobClassStats();
    }
    
    /**
     * {@inheritDoc}
     * 
     * @see azkaban.monitor.MonitorInterface#getCompletedWorkflowModels
     *     
     */
    public synchronized List<WorkflowExecutionModel> getCompletedWorkflowModels(
                                                         final long bufferTimeMS, 
                                                         final boolean reverseTime) {
        return eModel.getCompletedWorkflowModels(bufferTimeMS, reverseTime);
    }
    
    /**
     * {@inheritDoc}
     * 
     * @see azkaban.monitor.MonitorInterface#getNumberOfWorkflows
     *     
     */
    public synchronized long getNumberOfWorkflows() {
        return eModel.getNumberOfWorkflows();
    }
    
    /**
     * {@inheritDoc}
     * 
     * @see azkaban.monitor.MonitorInterface#getNumberOfCompletedWorkflows
     *     
     */
    public synchronized long getNumberOfCompletedWorkflows() {
        return eModel.getNumberOfCompletedWorkflows();
    }
    
    /**
     * {@inheritDoc}
     * 
     * @see azkaban.monitor.MonitorInterface#getCompletedWorkflowModels
     *     
     */
    public synchronized List<WorkflowExecutionModel> getCompletedWorkflowModels(final boolean reverseTime) {
        return eModel.getCompletedWorkflowModels(reverseTime);
    }
    
    /**
     * {@inheritDoc}
     * 
     * @see azkaban.monitor.MonitorInterface#clearCompletedWorkflows
     *     
     */
    public synchronized void clearCompletedWorkflows(final long bufferTimeMS) {
        clearCompletedWorkflows(bufferTimeMS);
    }
    
    /**
     * {@inheritDoc}
     * 
     * @see azkaban.monitor.MonitorInterface#clearCompletedWorkflows
     *     
     */
    public void clearCompletedWorkflows() {
       eModel.clearCompletedWorkflows(); 
    }
    
    /*************** Event interface (AzMonitorInternalInterface) ***************/

    /**
     * {@inheritDoc}
     * 
     * @see azkaban.monitor.MonitorInternalInterface#workflowEvent
     *     
     */
    public synchronized void workflowEvent(final String wfId,  
                                           final long time, 
                                           final WorkflowAction action, 
                                           final WorkflowState wfState,
                                           final String rootJobName) {
        switch (action) {
        case SCHEDULE_WORKFLOW:
            LOG.debug("Schedule workflow: " + rootJobName);
            eModel.scheduleWorkflow(rootJobName, time);
            break;
        case UNSCHEDULE_WORKFLOW:
            LOG.debug("Unscheduled workflow: " + rootJobName);
            // TODO
            break;
        case START_WORKFLOW:
            LOG.debug("Start workflow: " + wfId);
            eModel.startWorkflow(rootJobName, wfId, time);
            break;
        case END_WORKFLOW:
            if (wfState == WorkflowState.NOP) {
                throw new MonitorException("Nop cannot be used as a job state for EndWorkflow.");
            }
            eModel.endWorkflow(wfId, time, wfState);
            break;
        default:
            LOG.error("Illegal or unknown WorkflowAction specified.");
            break;
        }    
    }
    
    /**
     * {@inheritDoc}
     * 
     * @see azkaban.monitor.MonitorInternalInterface#jobEvent
     *     
     */
    public synchronized void jobEvent(final Job job,
                                      final long time, 
                                      final JobAction action, 
                                      final JobState jobState) {
        switch (action) {
        case START_WORKFLOW_JOB:
            LOG.debug("Start workflow job: " + job.getId());
            eModel.startWorkflowJob(time, job);
            break;
        case END_WORKFLOW_JOB:
            LOG.debug("End workflow job: " + job.getId());
            if (jobState == JobState.NOP) {
                throw new MonitorException("Nop cannot be used as a job state for EndWorkflowJob.");
            }
            eModel.endWorkflowJob(time, job, jobState);
            break;
        default:
            LOG.error("Unknown or illegal JobAction specified.");
            break;
        }
    }
    
    /**
     * {@inheritDoc}
     * 
     * @see azkaban.monitor.MonitorInternalInterface#workflowResourceThrottledJobEvent
     *     
     */
    public synchronized void workflowResourceThrottledJobEvent(final ResourceThrottledJob job,
                                                               final long lockWaitTime) {
        eModel.resourceWaitTime(lockWaitTime, job);       
    }
    
    //*************** Event notification registration (AzMonitorInterface) ***************//
    
    /**
     * {@inheritDoc}
     * 
     * @see azkaban.monitor.MonitorInterface#registerGlobalNotification
     *     
     */
    public synchronized void registerGlobalNotification(final MonitorListener notifier,
                                                        final GlobalNotificationType type) {
        NotificationDetails details = getNotifierDetails(notifier);
        switch(type) {
        case GLOBAL_STATS_CHANGE:
            details.setGlobalStatsMonitor(true);
            break;
        case ANY_WORKFLOW_CLASS_STATS_CHANGE:
            details.setAllWorkflowClassStatsMonitor(true);
            break;
        case ANY_JOB_CLASS_STATS_CHANGE:
            details.setAllJobClassStatsMonitor(true);
            break;
        default:
            LOG.error("Illegal or unknown GlobalNotificationType specified");
            break;
        }        
    }
    
    /**
     * {@inheritDoc}
     * 
     * @see azkaban.monitor.MonitorInterface#deregisterGlobalNotification
     *     
     */
    public synchronized void deregisterGlobalNotification(final MonitorListener notifier,
                                                          final GlobalNotificationType type) {
        NotificationDetails details = getNotifierDetails(notifier);
        switch(type) {
        case GLOBAL_STATS_CHANGE:
            details.setGlobalStatsMonitor(false);
            break;
        case ANY_WORKFLOW_CLASS_STATS_CHANGE:
            details.setAllWorkflowClassStatsMonitor(false);
            break;
        case ANY_JOB_CLASS_STATS_CHANGE:
            details.setAllJobClassStatsMonitor(false);
            break;
        default:
            LOG.error("Illegal or unknown GlobalNotificationType specified");
            break;
        }  
        
        // See if this notification is serving any purpose, and just remove if not.
        checkDoDeregisterNotification(notifier);
    }
   
    /**
     * Method to eliminate notifiers that aren't monitoring anything.
     * @param notifier
     *        MonitorListener we are examining.
     */
    private void checkDoDeregisterNotification(final MonitorListener notifier) {
        NotificationDetails details = getNotifierDetails(notifier);
        if (details == null) {
            return;
        }
        
        if (!details.hasAllJobClassStatsMonitor() && !details.hasAllJobClassStatsMonitor() 
                && !details.hasAllWorkflowClassStatsMonitor()
                && details.getGlobalWorkflowClasses().size() == 0
                && details.getGlobalJobClasses().size() == 0) {
            notifiers.remove(notifier);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see azkaban.monitor.MonitorInterface#deregisterNotifications
     *     
     */
    public synchronized void deregisterNotifications(final MonitorListener notifier) {
        NotificationDetails details = notifiers.get(notifier);
        if (details == null) {
            LOG.error("Attempt to deregister notification that never registered");
            return;
        }
        notifiers.remove(notifier);
        
        Set<String> wfClasses = details.getGlobalWorkflowClasses();
        for (String wfClassName : wfClasses) {
            List<MonitorListener> notifiers = workflowNotifiers.get(wfClassName);
            notifiers.remove(notifier);
            if (notifiers.size() == 0) {
                workflowNotifiers.remove(wfClassName);
            }
        }
        
        Set<String> jobClasses = details.getGlobalJobClasses();
        for (String jobClassName : jobClasses) {
            List<MonitorListener> notifiers = jobNotifiers.get(jobClassName);
            notifiers.remove(notifier);
            if (notifiers.size() == 0) {
                jobNotifiers.remove(jobClassName);
            }
        }
    }
    
    /**
     * {@inheritDoc}
     * 
     * @see azkaban.monitor.MonitorInterface#registerWorkflowClassNotification
     *     
     */
    public synchronized void registerWorkflowClassNotification(final MonitorListener notifier,
                                                               final String workflowClassId) { 
        registerClassNotification(notifier, workflowClassId, workflowNotifiers);
    }
    
    /**
     * {@inheritDoc}
     * 
     * @see azkaban.monitor.MonitorInterface#registerJobClassNotification
     *     
     */
    public synchronized void registerJobClassNotification(final MonitorListener notifier,
                                                          final String jobClassId) {
        registerClassNotification(notifier, jobClassId, jobNotifiers);        
    }
    
    /**
     * {@inheritDoc}
     * 
     * @see azkaban.monitor.MonitorInterface#registerClassNotification
     *     
     */
    private void registerClassNotification(final MonitorListener notifier, 
                                           final String classId, 
                                           final Map<String, List<MonitorListener>> notifierMap) {
        NotificationDetails details = getNotifierDetails(notifier);
        
        details.getGlobalWorkflowClasses().add(classId);

        
        List<MonitorListener> notifierList = notifierMap.get(classId);
        if (notifierList == null) {
            notifierList = new ArrayList<MonitorListener>();
            notifierMap.put(classId, notifierList);
        }
        
        if (!notifierList.contains(notifier)) {
            notifierList.add(notifier);
        }        
    }
    
    /**
     * {@inheritDoc}
     * 
     * @see azkaban.monitor.MonitorInterface#deregisterWorkflowClassNotification
     *     
     */
    public synchronized void deregisterWorkflowClassNotification(final MonitorListener notifier,
                                                                 final String workflowClassId) {
        deregisterClassNotification(notifier, workflowClassId, workflowNotifiers);
    }
    
    /**
     * {@inheritDoc}
     * 
     * @see azkaban.monitor.MonitorInterface#deregisterJobClassNotification
     *     
     */
    public synchronized void deregisterJobClassNotification(final MonitorListener notifier,
                                                            final String jobClassId) {
        deregisterClassNotification(notifier, jobClassId, jobNotifiers);
    }
    
    /**
     * Deregister a monitor class notifier.
     * @param notifier
     *        The notifier to deregister.
     * @param classId
     *        The root job id to deregister against.
     * @param notifierMap
     *        the map between classId and MonitorListeners to be adjusted.
     */
    private void deregisterClassNotification(final MonitorListener notifier, 
                                             final String classId, 
                                             final Map<String, List<MonitorListener>> notifierMap) {
        NotificationDetails details = getNotifierDetails(notifier);
        
        details.getGlobalWorkflowClasses().remove(classId);  
        
        List<MonitorListener> notifiers = notifierMap.get(classId);
        notifiers.remove(notifier); 
        if (notifiers.size() == 0) {
            notifierMap.remove(classId);
        }
        
        // See if this notification is serving any purpose, and just remove if not.
        checkDoDeregisterNotification(notifier);
    }
    
    /**
     * Used in testing.
     * @return a list of all registered MonitorNotification's.
     */
    protected synchronized List<MonitorListener> getAllNotifiers() {
        return new ArrayList<MonitorListener>(notifiers.keySet());
    }
    
    //*************** Event notification (AzMonitorInternalInterface) ***************//
    
    /**
     * {@inheritDoc}
     * 
     * @see azkaban.monitor.MonitorInternalInterface#executeGlobalNotify
     *     
     */
    public synchronized void executeGlobalNotify(final GlobalNotificationType eventType, 
                                                 final ClassStats statsObject) {
        switch (eventType) {
        case GLOBAL_STATS_CHANGE:
            for (Entry<MonitorListener, NotificationDetails> entry : notifiers.entrySet()) {
                if (entry.getValue().hasGlobalStatsMonitor()) {
                  entry.getKey().onGlobalNotify(eventType, statsObject);
                }
            }
            break;
        case ANY_WORKFLOW_CLASS_STATS_CHANGE:
            for (Entry<MonitorListener, NotificationDetails> entry : notifiers.entrySet()) {
                if (entry.getValue().hasAllWorkflowClassStatsMonitor()) {
                  entry.getKey().onGlobalNotify(eventType, statsObject);
                }
            }
            break;
        case ANY_JOB_CLASS_STATS_CHANGE:
            for (Entry<MonitorListener, NotificationDetails> entry : notifiers.entrySet()) {
                if (entry.getValue().hasAllJobClassStatsMonitor()) {
                  entry.getKey().onGlobalNotify(eventType, statsObject);
                }
            }
            break;
        default:
            LOG.error("Illegal or unknown GlobalNotificationType specified.");
            break;
        }
    } 
    
    /**
     * {@inheritDoc}
     * 
     * @see azkaban.monitor.MonitorInternalInterface#executeWorkflowClassNotify
     *     
     */
    public synchronized void executeWorkflowClassNotify(final NativeWorkflowClassStats wfStats) {
        List<MonitorListener> notifiers = 
            workflowNotifiers.get(wfStats.getWorkflowRootName());
        if (notifiers != null) {
            for (MonitorListener notifier : notifiers) {
                notifier.onWorkflowNotify(wfStats);
            }
        }
    }
    
    /**
     * {@inheritDoc}
     * 
     * @see azkaban.monitor.MonitorInternalInterface#executeJobClassNotify
     *     
     */
    public synchronized void executeJobClassNotify(final NativeJobClassStats jobStats) {
        List<MonitorListener> notifiers = 
            jobNotifiers.get(jobStats.getJobClassName());
        if (notifiers != null) {
            for (MonitorListener notifier : notifiers) {
                notifier.onJobNotify(jobStats);
            }
        }
    }
    
    /**
     * Get details about a given notifier.
     * @param notifier
     *        The MonitorListener whose details are requested.
     * @return
     *        NotificationDetails
     */
    private synchronized NotificationDetails getNotifierDetails(final MonitorListener notifier) {
        NotificationDetails details = notifiers.get(notifier);
        if (details == null) {
            details = new NotificationDetails(notifier);
            notifiers.put(notifier, details);
        }
        return details;
    }  
}
