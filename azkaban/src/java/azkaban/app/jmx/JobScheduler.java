/*
 * Copyright 2011 Adconion, Inc.
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
package azkaban.app.jmx;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Hours;
import org.joda.time.IllegalFieldValueException;
import org.joda.time.LocalDateTime;
import org.joda.time.Minutes;
import org.joda.time.ReadablePeriod;
import org.joda.time.Seconds;
import org.joda.time.format.DateTimeFormat;

import azkaban.app.JobDescriptor;
import azkaban.app.JobManager;
import azkaban.scheduler.ScheduleManager;

/**
 * @author ibrahimulukaya
 * Implements the JobSchedulerMBean
 */
public class JobScheduler implements JobSchedulerMBean {
    private static Logger logger = Logger.getLogger(JobScheduler.class);
    private ScheduleManager scheduler;
    private JobManager jobManager;

    public JobScheduler(ScheduleManager scheduler, JobManager jobManager) {
        this.scheduler = scheduler;
        this.jobManager = jobManager;
    }

    public String scheduleWorkflow(String jobName, boolean ignoreDeps,
            int hour, int minutes, int seconds, String scheduledDate,
            boolean isRecurring, int period, String periodUnits) {
        String errorMsg = null;
        if (jobName == null || jobName.trim().length() == 0) {
            errorMsg = "You must select at least one job to run.";
            logger.error(errorMsg);
            return errorMsg;
        }
        JobDescriptor descriptor = jobManager.getJobDescriptor(jobName);
        if (descriptor == null) {
            errorMsg = "Job: '" + jobName + "' doesn't exist.";
            logger.error(errorMsg);
            return errorMsg;
        }

        DateTime day = null;
        DateTime time = null;
        try {
            if (scheduledDate == null || scheduledDate.trim().length() == 0) {
                day = new LocalDateTime().toDateTime();
                time = day.withHourOfDay(hour).withMinuteOfHour(minutes)
                        .withSecondOfMinute(seconds);
                if (day.isAfter(time)) {
                    time = time.plusDays(1);
                }
            } else {
                try {
                    day = DateTimeFormat.forPattern("MM-dd-yyyy")
                            .parseDateTime(scheduledDate);
                } catch (IllegalArgumentException e) {
                    logger.error(e);
                    return "Invalid date: '" + scheduledDate
                            + "', \"MM-dd-yyyy\" format is expected.";
                }
                time = day.withHourOfDay(hour).withMinuteOfHour(minutes)
                        .withSecondOfMinute(seconds);
            }
        } catch (IllegalFieldValueException e) {
            logger.error(e);
            return "Invalid schedule time (see logs): " + e.getMessage();

        }
        ReadablePeriod thePeriod = null;
        if (isRecurring) {
            if ("d".equals(periodUnits)) {
                thePeriod = Days.days(period);
            } else if ("h".equals(periodUnits)) {
                thePeriod = Hours.hours(period);
            } else if ("m".equals(periodUnits)) {
                thePeriod = Minutes.minutes(period);
            } else if ("s".equals(periodUnits)) {
                thePeriod = Seconds.seconds(period);
            } else {
                errorMsg = "Unknown period unit: " + periodUnits;
                logger.error(errorMsg);
                return errorMsg;
            }
        }
        try {
            if (thePeriod == null) {
                scheduler.schedule(jobName, time, ignoreDeps);
            } else {
                scheduler.schedule(jobName, time, thePeriod, ignoreDeps);
            }
            return "Schedule Successful!";
        } catch (Exception e) {
            logger.error(e);
            return "Schedule Failed (see logs): " + e.getMessage();
        }
    }
    
    public String removeScheduledWorkflow(String jobName) {
        String errorMsg = null;
        if (jobName == null || jobName.trim().length() == 0) {
            errorMsg = "You must select at least one job to remove.";
            logger.error(errorMsg);
            return errorMsg;
        }
        if (scheduler.getSchedule(jobName) == null) {
            errorMsg = "Job: '"+ jobName + "' doesn't exist in schedule.";
            logger.error(errorMsg);
            return errorMsg;
        }
        try {
            scheduler.removeScheduledJob(jobName);
            return "Job: '" + jobName + "' is successfully removed from " +
                    "schedule.";
        } catch (Exception e) {
            logger.error(e);
            return "Removing From Schedule Failed (see logs): " + 
                    e.getMessage();
        }
    }
}
