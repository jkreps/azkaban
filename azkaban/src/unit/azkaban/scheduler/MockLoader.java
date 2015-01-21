package azkaban.scheduler;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.Period;

public class MockLoader implements ScheduleLoader {
	private ArrayList<ScheduledJob> scheduledJob = new ArrayList<ScheduledJob>();
	
	public void addScheduledJob(String id, DateTime time, Period recurrence, boolean dep) {
		ScheduledJob job = new ScheduledJob(id, time, recurrence, dep);
		addScheduleJob(job);
	}
	
	public void addScheduleJob(ScheduledJob job) {
		scheduledJob.add(job);
	}
	
	public void clearSchedule() {
		scheduledJob.clear();
	}
	
	@Override
	public List<ScheduledJob> loadSchedule() {
		return scheduledJob;
	}

	@Override
	public void saveSchedule(List<ScheduledJob> schedule) {
		scheduledJob.clear();
		scheduledJob.addAll(schedule);
	}
	
}