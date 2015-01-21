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

import java.util.concurrent.TimeUnit;

import azkaban.jobcontrol.impl.jobs.locks.JobLock;

/**
 * Test job lock that enforces a simple delay while acquiring it.
 */
public class TimeDelayJobLock implements JobLock {
    private long waitTimeMs;
    
    /**
     * constructor
     * @param waitTimeMs
     *            time in ms to simulate waiting on resources.
     */
    public TimeDelayJobLock(long waitTimeMs) {
        this.waitTimeMs = waitTimeMs;
    }

    @Override
    public void acquireLock() throws InterruptedException {
       Thread.sleep(waitTimeMs);       
    }

    @Override
    public long getLockAcquireTime(TimeUnit unit) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long getLockHeldTime(TimeUnit unit) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Status getStatus() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long getTotalLockTime(TimeUnit unit) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void releaseLock() {
        // TODO Auto-generated method stub
        
    }

}
