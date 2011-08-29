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

import azkaban.monitor.MonitorInterface.GlobalNotificationType;
import azkaban.monitor.stats.ClassStats;
import azkaban.monitor.stats.NativeJobClassStats;
import azkaban.monitor.stats.NativeWorkflowClassStats;

/**
 * Notification interface for updates on global, workflow, and job statistics.
 *
 */
public interface MonitorListener {
    
    /**
     * Notification regarding global state, all workflows, all jobs.
     * @param type
     *            the GlobalNotificationType representing the notification type./
     * @param statsObject 
     *            the statistics object that changed, global, workflow, or job class.
     */
    public void onGlobalNotify(GlobalNotificationType type, ClassStats statsObject);
    
    /**
     * Notification that workflow class statistics has been updated.
     * @param wfStats 
     *            a snapshot copy of a workflow class stats.
     */
    public void onWorkflowNotify(NativeWorkflowClassStats wfStats);
    
    /**
     * Notification that job class statistics has been updated.
     * @param jobStats 
     *            a snapshot copy of a job class stats.
     */
    public void onJobNotify(NativeJobClassStats jobStats);
}
