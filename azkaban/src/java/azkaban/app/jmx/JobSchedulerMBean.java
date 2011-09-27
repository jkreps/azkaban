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

/**
 * MXBean interface used to trigger a scheduling of a given job
 * @author ibrahimulukaya
 */
public interface JobSchedulerMBean {
    /**
     * Trigger a scheduling of a given job
     * 
     * @param jobName Identifier of the job
     * @param ignoreDeps Selection to ignore dependencies
     * @param hour Scheduled hour in 24-hour time
     * @param minutes Scheduled minutes
     * @param seconds Scheduled seconds
     * @param scheduledDate Scheduled date in "MM-dd-yyyy" format
     * Default date is today, "" or null means the default date.
     * @param isRecurring Selection to repeat job
     * @param period Repeating period
     * @param periodUnits Unit of the repeating period.
     * Can be day "d", hour "h", minute "m" or second "s"
     * 
     * @return "Schedule Successful!" on success, otherwise "Schedule Failed!"
     * and the text of the exception message.
     */
    @DisplayName("OPERATION: scheduleWorkflow - " +
            "Triggers a scheduling of a given job.")
    String scheduleWorkflow(
            @ParameterName("jobName : Identifier of the job")
            String jobName,
            @ParameterName("ignoreDeps : Selection to ignore dependencies")
            boolean ignoreDeps,
            @ParameterName("hour : Scheduled hour in 24-hour time")
            int hour,
            @ParameterName("minutes : Scheduled minutes")
            int minutes,
            @ParameterName("seconds : Scheduled seconds")
            int seconds,
            @ParameterName("scheduledDate : Scheduled date in \"MM-dd-yyyy\"" +
                    "format. \"\" or null means the default date.")
            String scheduledDate,
            @ParameterName("isRecurring : Selection to repeat job")
            boolean isRecurring,
            @ParameterName("period : Repeating period")
            int period,
            @ParameterName("periodUnits : Unit of the repeating period." +
                    " Can be day \"d\", hour \"h\", minute \"m\"" +
                    " or second \"s\"")
            String periodUnits);
    /**
     * Removes given job from schedule.
     * @param jobName Identifier of the job
     * 
     * @return "Job is succesfully removed from schedule!" on success,
     * otherwise "Removing From Schedule Failed!"
     * and the text of the exception message.
     */
    @DisplayName("OPERATION: removeScheduledWorkflow - " +
    "Removes given job from schedule.")
    String removeScheduledWorkflow(
            @ParameterName("jobName : Identifier of the job") String jobName);
}