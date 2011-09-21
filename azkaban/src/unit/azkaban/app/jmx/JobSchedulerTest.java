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

import java.util.ArrayList;

import org.joda.time.DateTime;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import azkaban.scheduler.MockJobExecutorManager;
import azkaban.scheduler.MockJobExecutorManager.ExecutionRecord;
import azkaban.scheduler.MockLoader;
import azkaban.scheduler.ScheduleManager;
import azkaban.scheduler.ScheduleManagerTest;

/**
 * @author ibrahimulukaya
 *
 */
public class JobSchedulerTest {
    private final MockJobExecutorManager executor = new MockJobExecutorManager();
    private final MockLoader loader = new MockLoader();
    private final MockJobManager jobManager = new MockJobManager();
    private ScheduleManager scheduler;
    private JobScheduler schedulerJMX;

    @Before
    public void setup(){
        scheduler = new ScheduleManager(executor, loader);
        schedulerJMX = new JobScheduler(scheduler, jobManager);
    }
    @Test
    public void testScheduleWorkflow() {
        // Setup and run for few mins.
        DateTime baseTime = new DateTime();
        baseTime = baseTime.minusMillis(baseTime.getMillisOfSecond()); 

        DateTime time2 = baseTime.plusSeconds(7);
        long period2Ms = 60000;
        schedulerJMX.scheduleWorkflow("test2", false, time2.getHourOfDay(), time2.getMinuteOfHour(), time2.getSecondOfMinute(), time2.toString("MM-dd-yyyy"), true, 1, "");
        schedulerJMX.scheduleWorkflow("test2", false, time2.getHourOfDay(), time2.getMinuteOfHour(), time2.getSecondOfMinute(), time2.toString("MM-dd-yyyy"), true, 1, "m");

        DateTime time4 = baseTime.plusSeconds(2);
        long period4Ms = 20000;
        schedulerJMX.scheduleWorkflow("test4", true, time4.getHourOfDay(), time4.getMinuteOfHour(), time4.getSecondOfMinute(), "", true, 20, "s");
        schedulerJMX.scheduleWorkflow("test4", true, 30, time4.getMinuteOfHour(), time4.getSecondOfMinute(), time4.toString("MM-dd-yyyy"), true, 20, "s");
        schedulerJMX.scheduleWorkflow("test4", true, time4.getHourOfDay(), time4.getMinuteOfHour(), time4.getSecondOfMinute(), time4.toString("MM-dd-yyyy"), true, 20, "s");

        DateTime time1 = baseTime.plusSeconds(5);
        long period1Ms = 100000;
        schedulerJMX.scheduleWorkflow("test1", false, time1.getHourOfDay(), time1.getMinuteOfHour(), time1.getSecondOfMinute(), "00-00-0000", true, 1, "h");
        schedulerJMX.scheduleWorkflow("test1", false, time1.getHourOfDay(), time1.getMinuteOfHour(), time1.getSecondOfMinute(), time1.toString("MM-dd-yyyy"), true, 1, "h");

        DateTime time3 = baseTime.plusSeconds(10);
        long period3Ms = 100000;
        schedulerJMX.scheduleWorkflow(null, false, time3.getHourOfDay(), time3.getMinuteOfHour(), time3.getSecondOfMinute(), time3.toString("MM-dd-yyyy"), true, 1, "d");
        schedulerJMX.scheduleWorkflow("test3", false, time3.getHourOfDay(), time3.getMinuteOfHour(), time3.getSecondOfMinute(), time3.toString("MM-dd-yyyy"), true, 1, "d");

        DateTime time6 = baseTime.plusSeconds(35); 
        schedulerJMX.scheduleWorkflow("test6", true, time6.getHourOfDay(), time6.getMinuteOfHour(), time6.getSecondOfMinute(), time6.toString("MM.dd.yyyy"), false, 0, "h");
        schedulerJMX.scheduleWorkflow(null, true, time6.getHourOfDay(), time6.getMinuteOfHour(), time6.getSecondOfMinute(), time6.toString("MM-dd-yyyy"), false, 0, "h");
        schedulerJMX.scheduleWorkflow("test6", true, time6.getHourOfDay(), time6.getMinuteOfHour(), time6.getSecondOfMinute(), time6.toString("MM-dd-yyyy"), false, 0, "h");

        long totalTime = 80000;
        DateTime endTime = new DateTime();
        endTime = endTime.plus(totalTime);
        for (int i = 0; i < totalTime; i+= 5000 ) { 
            synchronized(this) {
                try {
                    System.out.println("Tick Tock " + i);
                    wait(5000);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        scheduler.shutdown();

        // Check to see if jobs are scheduled with expected times and periods.  
        ArrayList<ExecutionRecord> executionRecords = executor.getExecutionList();
        ArrayList<ExecutionRecord> test1Results = ScheduleManagerTest.filterByName(executionRecords, "test1");
        ArrayList<ExecutionRecord> test2Results = ScheduleManagerTest.filterByName(executionRecords, "test2");
        ArrayList<ExecutionRecord> test3Results = ScheduleManagerTest.filterByName(executionRecords, "test3");
        ArrayList<ExecutionRecord> test4Results = ScheduleManagerTest.filterByName(executionRecords, "test4");
        ArrayList<ExecutionRecord> test6Results = ScheduleManagerTest.filterByName(executionRecords, "test6");

        ScheduleManagerTest.checkTimes(test1Results, ScheduleManagerTest.createCheckList(time1, period1Ms, endTime) );
        ScheduleManagerTest.checkTimes(test2Results, ScheduleManagerTest.createCheckList(time2, period2Ms, endTime) );
        ScheduleManagerTest.checkTimes(test3Results, ScheduleManagerTest.createCheckList(time3, period3Ms, endTime) );
        ScheduleManagerTest.checkTimes(test4Results, ScheduleManagerTest.createCheckList(time4, period4Ms, endTime) );
        ScheduleManagerTest.checkTimes(test6Results, new DateTime[] { time6 } );
    }
    
    @Test
    public void testJobExists()
    {
        DateTime baseTime = new DateTime();
        baseTime = baseTime.minusMillis(baseTime.getMillisOfSecond());
        DateTime time3 = baseTime.plusSeconds(10);
        long period3Ms = 100000;
        schedulerJMX.scheduleWorkflow(null, false, time3.getHourOfDay(), time3.getMinuteOfHour(), time3.getSecondOfMinute(), time3.toString("MM-dd-yyyy"), true, 1, "d");
        schedulerJMX.scheduleWorkflow("failtest", false, time3.getHourOfDay(), time3.getMinuteOfHour(), time3.getSecondOfMinute(), time3.toString("MM-dd-yyyy"), true, 1, "d");
        schedulerJMX.scheduleWorkflow("test3", false, time3.getHourOfDay(), time3.getMinuteOfHour(), time3.getSecondOfMinute(), time3.toString("MM-dd-yyyy"), true, 1, "d");
        long totalTime = 20000;
        DateTime endTime = new DateTime();
        endTime = endTime.plus(totalTime);
        for (int i = 0; i < totalTime; i+= 5000 ) { 
            synchronized(this) {
                try {
                    System.out.println("Tick Tock " + i);
                    wait(5000);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        scheduler.shutdown();
        // Check to see if jobs are scheduled with expected times and periods.
        ArrayList<ExecutionRecord> executionRecords = executor.getExecutionList();
        ArrayList<ExecutionRecord> test3Results = ScheduleManagerTest.filterByName(executionRecords, "test3");
        ScheduleManagerTest.checkTimes(test3Results, ScheduleManagerTest.createCheckList(time3, period3Ms, endTime) );
    }
    
    @Test
    public void testScheduleDate()
    {
        DateTime baseTime = new DateTime();
        baseTime = baseTime.minusMillis(baseTime.getMillisOfSecond());
        DateTime time5 = baseTime.minusSeconds(1);
        schedulerJMX.scheduleWorkflow("test5", false, time5.getHourOfDay(), time5.getMinuteOfHour(), time5.getSecondOfMinute(), null, false, 1, "m");
        time5 = time5.plusDays(1);
        DateTime time1 = baseTime.minusHours(1);        
        schedulerJMX.scheduleWorkflow("test1", false, time1.getHourOfDay(), time1.getMinuteOfHour(), time1.getSecondOfMinute(), "", false, 1, "m");
        time1 = time1.plusDays(1);
        
        long totalTime = 20000;
        DateTime endTime = new DateTime();
        endTime = endTime.plus(totalTime);
        for (int i = 0; i < totalTime; i+= 5000 ) { 
            synchronized(this) {
                try {
                    System.out.println("Tick Tock " + i);
                    wait(5000);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        scheduler.shutdown();

        // Check to see if jobs are scheduled with expected times and periods.  
        ArrayList<ExecutionRecord> executionRecords = executor.getExecutionList();
        ArrayList<ExecutionRecord> test5Results = ScheduleManagerTest.filterByName(executionRecords, "test5");
        ArrayList<ExecutionRecord> test1Results = ScheduleManagerTest.filterByName(executionRecords, "test1");
        ScheduleManagerTest.checkTimes(test5Results, new DateTime[] { } );
        ScheduleManagerTest.checkTimes(test1Results, new DateTime[] { } );
    }

    @Test
    public void testRemoveScheduledWorkflow() {
        DateTime baseTime = new DateTime();
        DateTime time1 = baseTime.plusSeconds(10);
        long period1Ms = 30000;
        Period period1 = new Period(period1Ms);
        scheduler.schedule("test1", time1, period1, false);

        DateTime time2 = baseTime.plusSeconds(15);
        scheduler.schedule("test2", time2, false);
        
        DateTime time3 = baseTime.plusSeconds(20);
        long period3Ms = 45000;
        Period period3 = new Period(period3Ms);
        scheduler.schedule("test3", time3, period3, false);
        
        DateTime time4 = baseTime.plusSeconds(5);
        long period4Ms = 20000;
        Period period4 = new Period(period4Ms);  
        scheduler.schedule("test4", time4, period4, true);

        schedulerJMX.removeScheduledWorkflow("test4");
        schedulerJMX.removeScheduledWorkflow("test4");
        schedulerJMX.removeScheduledWorkflow("test1");

        DateTime time5 = baseTime.plusSeconds(25);
        long period5Ms = 12000;
        Period period5 = new Period(period5Ms);
        scheduler.schedule("test4", time5, period5, false);

        long totalTime = 30000;
        DateTime endTime = new DateTime();
        endTime = endTime.plus(totalTime);
        for (int i = 0; i < totalTime; i+= 5000 ) { 
            synchronized(this) {
            try {

              System.out.println("Tick Tock " + i);
              wait(5000);
              } catch (InterruptedException e) {
                  e.printStackTrace();
              }
            }
        }
        scheduler.shutdown();
        
        // Check to see if jobs are scheduled with expected times and periods.
        ArrayList<ExecutionRecord> executionRecords = executor.getExecutionList();
        ArrayList<ExecutionRecord> test1Results = ScheduleManagerTest.filterByName(executionRecords, "test1");
        ArrayList<ExecutionRecord> test2Results = ScheduleManagerTest.filterByName(executionRecords, "test2");
        ArrayList<ExecutionRecord> test3Results = ScheduleManagerTest.filterByName(executionRecords, "test3");
        ArrayList<ExecutionRecord> test4Results = ScheduleManagerTest.filterByName(executionRecords, "test4");
        
        ScheduleManagerTest.checkTimes(test1Results, ScheduleManagerTest.createCheckList(time1, period1Ms, baseTime) );
        ScheduleManagerTest.checkTimes(test2Results, new DateTime[] { time2 } );
        ScheduleManagerTest.checkTimes(test3Results, ScheduleManagerTest.createCheckList(time3, period3Ms, endTime) );
        ScheduleManagerTest.checkTimes(test4Results, ScheduleManagerTest.createCheckList(time5, period5Ms, endTime) );
    }
    
    @Test
    public void testUnscheduleEmptyWF() {
        String returnMsg = "You must select at least one job to remove.";
        Assert.assertEquals("Expected behaviour", schedulerJMX.removeScheduledWorkflow(""), returnMsg);
        Assert.assertEquals("Expected behaviour", schedulerJMX.removeScheduledWorkflow(null), returnMsg);
    }

    @Test
    public void testUnscheduleNotScheduledWF() {
        String jobName = "test7";
        String returnMsg = "Job: '"+ jobName + "' doesn't exist in schedule.";
        Assert.assertEquals("Expected behaviour", schedulerJMX.removeScheduledWorkflow(jobName), returnMsg);
    }
}
