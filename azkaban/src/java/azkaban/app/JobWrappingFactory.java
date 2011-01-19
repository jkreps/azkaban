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

package azkaban.app;

import azkaban.common.jobs.Job;
import azkaban.common.utils.Utils;
import azkaban.jobcontrol.impl.jobs.ResourceThrottledJob;
import azkaban.jobcontrol.impl.jobs.RetryingJob;
import azkaban.jobcontrol.impl.jobs.locks.GroupLock;
import azkaban.jobcontrol.impl.jobs.locks.JobLock;
import azkaban.jobcontrol.impl.jobs.locks.NamedPermitManager;
import azkaban.jobcontrol.impl.jobs.locks.PermitLock;
import azkaban.jobcontrol.impl.jobs.locks.ReadWriteLockManager;
import azkaban.jobs.JobExecutionException;

import com.google.common.base.Function;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JobWrappingFactory implements Function<JobDescriptor, Job>
{
    private final ReadWriteLockManager _readWriteLockManager;
    private final String _logDir;
    //private final String _defaultType;
    private final Map<String, Class<? extends Job>> _jobToClass;

    private final NamedPermitManager _permitManager;

    public JobWrappingFactory(
            final NamedPermitManager permitManager,
            final ReadWriteLockManager readWriteLockManager,
            final String logDir,
            final String defaultType,
            final Map<String, Class<? extends Job>> jobTypeToClassMap
    )
    {
        this._permitManager = permitManager;
        this._readWriteLockManager = readWriteLockManager;
        this._logDir = logDir;
        //this._defaultType = defaultType;
        this._jobToClass = jobTypeToClassMap;
    }

    @Override
    public Job apply(JobDescriptor jobDescriptor)
    {
      
      Job job;
      try {
        String jobType = jobDescriptor.getJobType();
        if (jobType == null || jobType.length() == 0) {
           /*throw an exception when job name is null or empty*/
          throw new JobExecutionException (
                                           String.format("The 'type' parameter for job[%s] is null or empty", jobDescriptor));
        }
        Class<? extends Object> executorClass = _jobToClass.get(jobType);

        if (executorClass == null) {
            throw new JobExecutionException(
                    String.format(
                            "Could not construct job[%s] of type[%s].",
                            jobDescriptor,
                            jobType
                    ));
        }
        
        job = (Job)Utils.callConstructor(executorClass, jobDescriptor);

        // wrap up job in retrying proxy if necessary
        if(jobDescriptor.getRetries() > 0)
            job = new RetryingJob(job, jobDescriptor.getRetries(), jobDescriptor.getRetryBackoffMs());

        // Group Lock List
        ArrayList<JobLock> jobLocks = new ArrayList<JobLock>();

        // If this job requires work permits wrap it in a resource throttler
        if(jobDescriptor.getNumRequiredPermits() > 0) {
            PermitLock permits = _permitManager.getNamedPermit("default",
                                                               jobDescriptor.getNumRequiredPermits());
            if(permits == null) {
                throw new RuntimeException("Job " + jobDescriptor.getId() + " requires non-existant default");
            } else if(permits.getDesiredNumPermits() > permits.getTotalNumberOfPermits()) {
                throw new RuntimeException(
                        String.format(
                                "Job %s requires %s but azkaban only has a total of %s permits, so this job cannot ever run.",
                                jobDescriptor.getId(),
                                permits.getTotalNumberOfPermits(),
                                permits.getDesiredNumPermits()
                        )
                );
            } else {
                jobLocks.add(permits);
            }
        }

        if(jobDescriptor.getReadResourceLocks() != null) {
            List<String> readLocks = jobDescriptor.getReadResourceLocks();
            for(String resource: readLocks) {
                jobLocks.add(_readWriteLockManager.getReadLock(resource));
            }
        }
        if(jobDescriptor.getWriteResourceLocks() != null) {
            List<String> writeLocks = jobDescriptor.getWriteResourceLocks();
            for(String resource: writeLocks) {
                jobLocks.add(_readWriteLockManager.getWriteLock(resource));
            }
        }

        if(jobLocks.size() > 0) {
            // Group lock
            GroupLock groupLock = new GroupLock(jobLocks);
            job = new ResourceThrottledJob(job, groupLock);
        }

      }
      catch (Exception e) {
          job = new InitErrorJob(jobDescriptor.getId(), e);
      }

        // wrap up job in logging proxy
        if (jobDescriptor.getLoggerPattern() != null) {
        	job = new LoggingJob(_logDir, job, job.getId(), jobDescriptor.getLoggerPattern());	
        }
        else {
        	job = new LoggingJob(_logDir, job, job.getId());	
        }
        
        return job;
    }
}