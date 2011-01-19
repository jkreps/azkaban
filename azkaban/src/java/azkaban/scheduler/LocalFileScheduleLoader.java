package azkaban.scheduler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.DurationFieldType;
import org.joda.time.Hours;
import org.joda.time.Minutes;
import org.joda.time.ReadablePeriod;
import org.joda.time.Seconds;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import azkaban.common.utils.Props;
import azkaban.util.json.JSONUtils;

/**
 * Loads the schedule from a schedule file that is JSON like. The format would be as follows:
 * 
 * {
 * 		schedule: [
 * 			{
 * 				"id": "<id>",
 * 				"time": "<time>",
 * 				"recurrence":"<period>",
 * 				"dependency":<boolean>
 * 			}
 * 		]
 * }	
 * 
 * 
 * @author rpark
 *
 */
public class LocalFileScheduleLoader implements ScheduleLoader {
	private static final String ID = "id";
	private static final String SCHEDULE = "schedule";
	private static final String TIME = "time";
	private static final String TIMEZONE = "timezone";
	private static final String RECURRENCE = "recurrence";
	private static final String IGNORE_DEPENDENCY = "ignoreDependency";
	
	private static DateTimeFormatter FILE_DATEFORMAT = DateTimeFormat.forPattern("yyyy-MM-dd.HH.mm.ss.SSS");
    private static Logger logger = Logger.getLogger(LocalFileScheduleLoader.class);

	private File scheduleFile;
	private File backupScheduleFile;
	
	private JSONUtils jsonUtils = new JSONUtils();
	
	public LocalFileScheduleLoader(File schedule, File backupSchedule) {
		this.scheduleFile = schedule;
		this.backupScheduleFile = backupSchedule;
	}


	@Override
	public List<ScheduledJob> loadSchedule() {
        if (scheduleFile != null && backupScheduleFile != null) {
            if (scheduleFile.exists()) {
				return loadFromFile(scheduleFile);
            }
            else if (backupScheduleFile.exists()) {
            	backupScheduleFile.renameTo(scheduleFile);
				return loadFromFile(scheduleFile);
            }
            else {
                logger.warn("No schedule files found looking for " + scheduleFile.getAbsolutePath());
            }
        }
        
        return new ArrayList<ScheduledJob>();
	}

	@Override
	public void saveSchedule(List<ScheduledJob> schedule) {
        if (scheduleFile != null && backupScheduleFile != null) {
            // Delete the backup if it exists and a current file exists.
            if (backupScheduleFile.exists() && scheduleFile.exists()) {
            	backupScheduleFile.delete();
            }

            // Rename the schedule if it exists.
            if (scheduleFile.exists()) {
            	scheduleFile.renameTo(backupScheduleFile);
            }

            HashMap<String,Object> obj = new HashMap<String,Object>();
            ArrayList<Object> schedules = new ArrayList<Object>();
            obj.put(SCHEDULE, schedules);
            //Write out schedule.
                       
            for (ScheduledJob schedJob : schedule) {
            	schedules.add(createJSONObject(schedJob));
            }
 
    		try {
    			FileWriter writer = new FileWriter(scheduleFile);
    			writer.write(jsonUtils.toJSONString(obj, 4));
    			writer.flush();
    		} catch (Exception e) {
    			throw new RuntimeException("Error saving flow file", e);
    		}
        }
	}
	
    @SuppressWarnings("unchecked")
	private List<ScheduledJob> loadFromFile(File schedulefile)
    {
    	BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(schedulefile));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			logger.error("Error loading schedule file ", e);
		}
    	List<ScheduledJob> scheduleList = new ArrayList<ScheduledJob>();
    	
		HashMap<String, Object> schedule;
		try {
			schedule = (HashMap<String,Object>)jsonUtils.fromJSONStream(reader);
		} catch (Exception e) {
			schedule = loadLegacyFile(schedulefile);
		}
		finally {
			try {
				reader.close();
			} catch (IOException e) {
			}
		}
		
		ArrayList<Object> array = (ArrayList<Object>)schedule.get("schedule");
		for (int i = 0; i < array.size(); ++i) {
			HashMap<String, Object> schedItem = (HashMap<String, Object>)array.get(i);
			ScheduledJob sched = createScheduledJob(schedItem);
			if (sched != null) {
				scheduleList.add(sched);	
			}
		}
		
		return scheduleList;
    }

    private ScheduledJob createScheduledJob(HashMap<String, Object> obj) {
    	String id = (String)obj.get(ID);
    	String time = (String)obj.get(TIME);
    	String timezone = (String)obj.get(TIMEZONE);
    	String recurrence = (String)obj.get(RECURRENCE);
    	Boolean dependency = (Boolean)obj.get(IGNORE_DEPENDENCY);
    	if (dependency == null) {
    		dependency = false;
    	}
    	
    	DateTime dateTime = FILE_DATEFORMAT.parseDateTime(time);

    	if (dateTime == null) {
    		logger.error("No time has been set");
    		return null;
    	}
    	if (timezone != null) {
    		dateTime = dateTime.withZoneRetainFields(DateTimeZone.forID(timezone));
    	}

        ReadablePeriod period = null;
    	if (recurrence != null) {
    		period = parsePeriodString(id, recurrence);
    	}

    	ScheduledJob scheduledJob = new ScheduledJob(id, dateTime, period, dependency);
    	if (scheduledJob.updateTime()) {
    		return scheduledJob;
    	}
    	
    	logger.info("Removed " + id + " off out of scheduled. It is not recurring.");
    	return null;
    }
    
	private HashMap<String,Object> createJSONObject(ScheduledJob job) {
    	HashMap<String,Object> object = new HashMap<String,Object>();
    	object.put(ID, job.getId());
    	object.put(TIME, FILE_DATEFORMAT.print(job.getScheduledExecution()));
    	object.put(TIMEZONE, job.getScheduledExecution().getZone().getID());
    	object.put(RECURRENCE, createPeriodString(job.getPeriod()));
    	object.put(IGNORE_DEPENDENCY, job.isDependencyIgnored());
    	
    	return object;
    }
    
    private ReadablePeriod parsePeriodString(String jobname, String periodStr)
    {
        ReadablePeriod period;
        char periodUnit = periodStr.charAt(periodStr.length() - 1);
        if (periodUnit == 'n') {
            return null;
        }

        int periodInt = Integer.parseInt(periodStr.substring(0, periodStr.length() - 1));
        switch (periodUnit) {
            case 'd':
                period = Days.days(periodInt);
                break;
            case 'h':
                period = Hours.hours(periodInt);
                break;
            case 'm':
                period = Minutes.minutes(periodInt);
                break;
            case 's':
                period = Seconds.seconds(periodInt);
                break;
            default:
                throw new IllegalArgumentException("Invalid schedule period unit '" + periodUnit + "' for job " + jobname);
        }

        return period;
    }

    private String createPeriodString(ReadablePeriod period)
    {
        String periodStr = "n";

        if (period == null) {
            return "n";
        }

        if (period.get(DurationFieldType.days()) > 0) {
            int days = period.get(DurationFieldType.days());
            periodStr = days + "d";
        }
        else if (period.get(DurationFieldType.hours()) > 0) {
            int hours = period.get(DurationFieldType.hours());
            periodStr = hours + "h";
        }
        else if (period.get(DurationFieldType.minutes()) > 0) {
            int minutes = period.get(DurationFieldType.minutes());
            periodStr = minutes + "m";
        }
        else if (period.get(DurationFieldType.seconds()) > 0) {
            int seconds = period.get(DurationFieldType.seconds());
            periodStr = seconds + "s";
        }

        return periodStr;
    }

    private HashMap<String,Object> loadLegacyFile(File schedulefile) {
        Props schedule = null;
        try {
            schedule = new Props(null, schedulefile.getAbsolutePath());
        } catch(Exception e) {
            throw new RuntimeException("Error loading schedule from " + schedulefile);
        }

        ArrayList<Object> jobScheduleList = new ArrayList<Object>();
        for(String key: schedule.getKeySet()) {
        	HashMap<String,Object> scheduledMap = parseScheduledJob(key, schedule.get(key));
        	if (scheduledMap == null) {
        		jobScheduleList.add(scheduledMap);
        	}
        }

        HashMap<String,Object> scheduleMap = new HashMap<String,Object>();
        scheduleMap.put(SCHEDULE, jobScheduleList );
        
        return scheduleMap;
    }
    
    private HashMap<String,Object> parseScheduledJob(String name, String job) {
        String[] pieces = job.split("\\s+");

        if(pieces.length != 3) {
            logger.warn("Error loading schedule from file " + name);
            return null;
        }

        HashMap<String,Object> scheduledJob = new HashMap<String,Object>();
        scheduledJob.put(ID, name);
        scheduledJob.put(TIME, pieces[0]);
        scheduledJob.put(RECURRENCE, pieces[1]);
        Boolean dependency = Boolean.parseBoolean(pieces[2]);
        if(dependency == null) {
            dependency = false;
        }
        scheduledJob.put(IGNORE_DEPENDENCY, dependency);     
        
        return scheduledJob;
    }
}