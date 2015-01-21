package azkaban.scheduler;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import azkaban.scheduler.MockJobExecutorManager.ExecutionRecord;

public class ScheduleManagerTest {
    @Test
    public void testLoadScheduleRegular() {
        MockJobExecutorManager executor = new MockJobExecutorManager();
        MockLoader loader = new MockLoader();

        // Setup and run for few mins.

        DateTime baseTime = new DateTime();
        DateTime time1 = baseTime.plusSeconds(5);
        long period1Ms = 30000;
        Period period1 = new Period(period1Ms);
        loader.addScheduledJob("test1", time1, period1, false);

        DateTime time2 = baseTime.plusSeconds(15);
        loader.addScheduledJob("test2", time2, null, false);

        DateTime time3 = baseTime.plusSeconds(10);
        long period3Ms = 45000;
        Period period3 = new Period(period3Ms);
        loader.addScheduledJob("test3", time3, period3, false);

        DateTime time5 = baseTime.minusSeconds(25);
        long period5Ms = 12000;
        Period period5 = new Period(period5Ms);
        loader.addScheduledJob("test5", time5, period5, false);
        while (time5.isBefore(baseTime)) {
            time5 = time5.plus(period5);
        }

        ScheduleManager scheduler = new ScheduleManager(executor, loader);

        synchronized (this) {
            try {
                wait(500);
            } catch (InterruptedException e) {
            }
        }

        DateTime time4 = baseTime.plusSeconds(2);
        long period4Ms = 20000;
        Period period4 = new Period(period4Ms);
        scheduler.schedule("test4", time4, period4, true);

        DateTime time6 = baseTime.plusSeconds(35);
        scheduler.schedule("test6", time6, true);

        long totalTime = 45000;
        DateTime endTime = new DateTime();
        endTime = endTime.plus(totalTime);
        for (int i = 0; i < totalTime; i += 5000) {
            synchronized (this) {
                try {

                    System.out.println("Tick Tock " + i);
                    wait(5000);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
        scheduler.shutdown();

        // Check to see that
        ArrayList<ExecutionRecord> executionRecords = executor
                .getExecutionList();
        ArrayList<ExecutionRecord> test1Results = filterByName(
                executionRecords, "test1");
        ArrayList<ExecutionRecord> test2Results = filterByName(
                executionRecords, "test2");
        ArrayList<ExecutionRecord> test3Results = filterByName(
                executionRecords, "test3");
        ArrayList<ExecutionRecord> test4Results = filterByName(
                executionRecords, "test4");
        ArrayList<ExecutionRecord> test5Results = filterByName(
                executionRecords, "test5");
        ArrayList<ExecutionRecord> test6Results = filterByName(
                executionRecords, "test6");

        checkTimes(test1Results, createCheckList(time1, period1Ms, endTime));
        checkTimes(test2Results, new DateTime[] { time2 });
        checkTimes(test3Results, createCheckList(time3, period3Ms, endTime));
        checkTimes(test4Results, createCheckList(time4, period4Ms, endTime));
        checkTimes(test5Results, createCheckList(time5, period5Ms, endTime));
        checkTimes(test6Results, new DateTime[] { time6 });
    }

    public static DateTime[] createCheckList(final DateTime startTime,
            final long repeating, final DateTime endTime) {

        ArrayList<DateTime> checkList = new ArrayList<DateTime>();

        DateTime start = startTime;
        while (start.isBefore(endTime)) {
            checkList.add(start);
            start = start.plus(repeating);
        }

        return checkList.toArray(new DateTime[checkList.size()]);
    }

    public static ArrayList<ExecutionRecord> filterByName(
            final ArrayList<ExecutionRecord> executionRecords, final String name) {
        ArrayList<ExecutionRecord> filteredRecords = new ArrayList<ExecutionRecord>();
        for (ExecutionRecord record : executionRecords) {
            if (record.getId().equals(name)) {
                filteredRecords.add(record);
            }
        }

        return filteredRecords;
    }

    public static void checkTimes(final List<ExecutionRecord> executionRecords,
            final DateTime[] times) {
        Assert.assertEquals("Sizes are the same as expected",
                executionRecords.size(), times.length);

        for (int i = 0; i < executionRecords.size(); ++i) {
            Assert.assertTrue(
                    "Unexpected execution",
                    areTimesEqual(executionRecords.get(i).getStartTime(),
                            times[i], 100));
        }
    }

    public static boolean areTimesEqual(final DateTime a, final DateTime b,
            final long msThreshold) {
        Duration duration = new Duration(a, b);
        return Math.abs(duration.getMillis()) < msThreshold;
    }

    @Test
    public void testLoadRemoveSchedule() {
        MockJobExecutorManager executor = new MockJobExecutorManager();
        MockLoader loader = new MockLoader();

        DateTime baseTime = new DateTime();
        DateTime time1 = baseTime.plusSeconds(10);
        long period1Ms = 30000;
        Period period1 = new Period(period1Ms);
        loader.addScheduledJob("test1", time1, period1, false);

        DateTime time2 = baseTime.plusSeconds(15);
        loader.addScheduledJob("test2", time2, null, false);

        DateTime time3 = baseTime.plusSeconds(20);
        long period3Ms = 45000;
        Period period3 = new Period(period3Ms);
        loader.addScheduledJob("test3", time3, period3, false);

        DateTime time4 = baseTime.plusSeconds(5);
        long period4Ms = 20000;
        Period period4 = new Period(period4Ms);
        loader.addScheduledJob("test4", time4, period4, true);

        DateTime time5 = baseTime.minusSeconds(25);
        long period5Ms = 12000;
        Period period5 = new Period(period5Ms);
        loader.addScheduledJob("test5", time5, period5, false);
        while (time5.isBefore(baseTime)) {
            time5 = time5.plus(period5);
        }

        ScheduleManager scheduler = new ScheduleManager(executor, loader);

        synchronized (this) {
            try {
                wait(3000);
            } catch (InterruptedException e) {
            }
        }

        scheduler.removeScheduledJob("test4");
        scheduler.removeScheduledJob("test5");

        DateTime time6 = baseTime.plusSeconds(25);
        long period6Ms = 12000;
        Period period6 = new Period(period6Ms);
        scheduler.schedule("test5", time6, period6, false);

        long totalTime = 30000;
        DateTime endTime = new DateTime();
        endTime = endTime.plus(totalTime);
        for (int i = 0; i < totalTime; i += 5000) {
            synchronized (this) {
                try {

                    System.out.println("Tick Tock " + i);
                    wait(5000);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
        scheduler.shutdown();

        ArrayList<ExecutionRecord> executionRecords = executor
                .getExecutionList();
        ArrayList<ExecutionRecord> test1Results = filterByName(
                executionRecords, "test1");
        ArrayList<ExecutionRecord> test2Results = filterByName(
                executionRecords, "test2");
        ArrayList<ExecutionRecord> test3Results = filterByName(
                executionRecords, "test3");
        ArrayList<ExecutionRecord> test4Results = filterByName(
                executionRecords, "test4");
        ArrayList<ExecutionRecord> test5Results = filterByName(
                executionRecords, "test5");

        checkTimes(test1Results, createCheckList(time1, period1Ms, endTime));
        checkTimes(test2Results, new DateTime[] { time2 });
        checkTimes(test3Results, createCheckList(time3, period3Ms, endTime));
        checkTimes(test4Results,
                createCheckList(time4, period4Ms, baseTime.plus(3000)));
        checkTimes(test5Results, createCheckList(time6, period6Ms, endTime));
    }
}