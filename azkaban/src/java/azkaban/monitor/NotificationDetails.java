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

import java.util.HashSet;
import java.util.Set;

/**
 * Class used to track the types of events a notifier has requested.
 * These types fall into the following categories:
 *   1) Global, all workflows, all jobs.
 *   2) Specific workflow classes.
 *   3) Specific job classes.
 *
 */
public class NotificationDetails {
    
    /**
     * The notifier whose notifications are being enumerated.
     */
    private MonitorListener notifier;
    
    /**
     * Set if interested in global statistics.
     */
    private boolean globalStatsMonitor;
    
    /**
     * Set if interested in notification on all workflow classes.
     */
    private boolean allWorkflowClassStatsMonitor;
    
    /**
     * Set if interested in notification on all job classes.
     */
    private boolean allJobClassStatsMonitor;
    
    /**
     * Set of all workflow classes of which notifier is interested.
     */
    private Set<String> globalWorkflowClasses = new HashSet<String>();
    
    /** 
     * Set of all job classes of which notifier is interested.
     */
    private Set<String> globalJobClasses = new HashSet<String>();
    
    /**
     * Constructor.
     * @param notifier
     *            the notifier whose details we will collect in this instance.
     */
    public NotificationDetails(MonitorListener notifier) { 
        this.notifier = notifier;
    }
    
    /**
     * setter
     * @param tf
     *            set if has global stats notification.
     */
    public void setGlobalStatsMonitor(boolean tf) {
        globalStatsMonitor = tf;
    }
    
    /**
     * getter
     * @return true if has global stats notification.
     */
    public boolean hasGlobalStatsMonitor() {
        return globalStatsMonitor;
    }
    
    /**
     * setter
     * @param tf
     *            set if has all workflow class changes notification.
     */
    public void setAllWorkflowClassStatsMonitor(boolean tf) {
        allWorkflowClassStatsMonitor = tf;
    }
    
    /**
     * getter
     * @return true if has all workflow class stats change notification.
     */
    public boolean hasAllWorkflowClassStatsMonitor() {
        return allWorkflowClassStatsMonitor;
    }
    
    /**
     * setter
     * @param tf
     *            set if has all job class changes notification.
     */
    public void setAllJobClassStatsMonitor(boolean tf) {
        allJobClassStatsMonitor = tf;
    }
    
    /**
     * getter
     * @return true if has all job class stats change notification.
     */
    public boolean hasAllJobClassStatsMonitor() {
        return allJobClassStatsMonitor;
    }
    
    /**
     * getter
     * @return set of names of all workflow root jobs notifier responds 
     *         specifically to.
     */
    public Set<String> getGlobalWorkflowClasses() {
        return globalWorkflowClasses;
    }
    
    /**
     * getter
     * @return set of names of all job class names notifier responds
     *         to specifically.
     */
    public Set<String> getGlobalJobClasses() {
        return globalJobClasses;
    }
    
    /**
     * getter
     * @return the notifier which is th focus of this instance.
     */
    public MonitorListener getNotifier() {
        return notifier;
    }

}
