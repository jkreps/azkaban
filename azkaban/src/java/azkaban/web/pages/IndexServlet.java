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

package azkaban.web.pages;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Hours;
import org.joda.time.LocalDateTime;
import org.joda.time.Minutes;
import org.joda.time.ReadablePeriod;
import org.joda.time.Seconds;
import org.joda.time.format.DateTimeFormat;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import azkaban.app.AzkabanApplication;
import azkaban.app.JobDescriptor;
import azkaban.app.JobManager;
import azkaban.common.web.Page;
import azkaban.flow.ExecutableFlow;
import azkaban.flow.Flow;
import azkaban.flow.FlowManager;
import azkaban.jobs.JobExecutionException;
import azkaban.jobs.JobExecutorManager.ExecutingJobAndInstance;
import azkaban.util.json.JSONUtils;
import azkaban.web.AbstractAzkabanServlet;

/**
 * The main page
 * 
 * @author jkreps
 * 
 */
public class IndexServlet extends AbstractAzkabanServlet {

    private static final Logger logger = Logger.getLogger(IndexServlet.class.getName());

    private static final long serialVersionUID = 1;

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
            IOException {
        /* set runtime properties from request and response */
        super.setRuntimeProperties(req, resp);

        AzkabanApplication app = getApplication();
        @SuppressWarnings("unused")
        Map<String, JobDescriptor> descriptors = app.getJobManager().loadJobDescriptors();
        Page page = newPage(req, resp, "azkaban/web/pages/index.vm");
        page.add("logDir", app.getLogDirectory());
        page.add("flows", app.getAllFlows());
        page.add("scheduled", app.getScheduleManager().getSchedule());
        page.add("executing", app.getJobExecutorManager().getExecutingJobs());
        page.add("completed", app.getJobExecutorManager().getCompleted());
        page.add("rootJobNames", app.getAllFlows().getRootFlowNames());
        page.add("folderNames", app.getAllFlows().getFolders());
        page.add("jobDescComparator", JobDescriptor.NAME_COMPARATOR);
        page.render();
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {

        /* set runtime properties from request and response */
        super.setRuntimeProperties(req, resp);

        AzkabanApplication app = getApplication();
        String action = getParam(req, "action");
        if ("loadjobs".equals(action)) {
        	resp.setContentType("application/json");
        	String folder = getParam(req, "folder");
        	resp.getWriter().print(getJSONJobsForFolder(app.getAllFlows(), folder));
        	resp.getWriter().flush();
        	return;
        }
        else if("unschedule".equals(action)) {
            String jobid = getParam(req, "job");
            app.getScheduleManager().removeScheduledJob(jobid);
        } else if("cancel".equals(action)) {
            cancelJob(app, req);
        } else if("schedule".equals(action)) {
            String redirect = scheduleJobs(app, req, resp);
            if (!redirect.isEmpty()) {
            	resp.sendRedirect(redirect);
            	return;
            }
        } else {
            throw new ServletException("Unknown action: " + action);
        }
        resp.sendRedirect(req.getContextPath());
    }
    
    @SuppressWarnings("unchecked")
	private String getJSONJobsForFolder(FlowManager manager, String folder) {
    	List<String> rootJobs = manager.getRootNamesByFolder(folder);
    	Collections.sort(rootJobs);

    	JSONArray rootJobObj = new JSONArray();
    	for (String root: rootJobs) {
    		Flow flow = manager.getFlow(root);
    		JSONObject flowObj = getJSONDependencyTree(flow);
    		rootJobObj.add(flowObj);
    	}
    	
    	return rootJobObj.toJSONString();
    }
    
    @SuppressWarnings("unchecked")
	private JSONObject getJSONDependencyTree(Flow flow) {
    	JSONObject jobObject = new JSONObject();
    	jobObject.put("name", flow.getName());
    	
    	if (flow.hasChildren()) {
    		JSONArray dependencies = new JSONArray();
    		for(Flow child : flow.getChildren()) {
    			JSONObject childObj = getJSONDependencyTree(child);
    			dependencies.add(childObj);
    		}
    		
    		Collections.sort(dependencies, new FlowComparator());
    		jobObject.put("dep", dependencies);
    	}
    	
    	return jobObject;
    }
    
    private class FlowComparator implements Comparator<JSONObject> {

		@Override
		public int compare(JSONObject arg0, JSONObject arg1) {
			String first = (String)arg0.get("name");
			String second = (String)arg1.get("name");
			return first.compareTo(second);
		}
    	
    }
    
    private void cancelJob(AzkabanApplication app, HttpServletRequest req) throws ServletException {

        String jobId = getParam(req, "job");
        try {
			app.getJobExecutorManager().cancel(jobId);
		} catch (Exception e1) {
			logger.error("Error cancelling job " + e1);
		}
        
        Collection<ExecutingJobAndInstance> executing = app.getJobExecutorManager().getExecutingJobs();
        for(ExecutingJobAndInstance curr: executing) {
            ExecutableFlow flow = curr.getExecutableFlow();
            final String flowId = flow.getId();
            if(flowId.equals(jobId)) {
                final String flowName = flow.getName();
                try {
                    if(flow.cancel()) {
                        addMessage(req, "Cancelled " + flowName);
                        logger.info("Job '" + flowName + "' cancelled from gui.");
                    } else {
                        logger.info("Couldn't cancel flow '" + flowName + "' for some reason.");
                        addError(req, "Failed to cancel flow " + flowName + ".");
                    }
                } catch(Exception e) {
                    logger.error("Exception while attempting to cancel flow '" + flowName + "'.", e);
                    addError(req, "Failed to cancel flow " + flowName + ": " + e.getMessage());
                }
            }
        }
    }

    private String scheduleJobs(AzkabanApplication app,
                              HttpServletRequest req,
                              HttpServletResponse resp) throws IOException, ServletException {
        String[] jobNames = req.getParameterValues("jobs");
        if(!hasParam(req, "jobs")) {
            addError(req, "You must select at least one job to run.");
            return "";
        }
        
        if (hasParam(req, "flow_now")) {
        	if (jobNames.length > 1) {
        		addError(req, "Can only run flow instance on one job.");
                return "";
        	}
        	
        	String jobName = jobNames[0];
            JobManager jobManager = app.getJobManager();
            JobDescriptor descriptor = jobManager.getJobDescriptor(jobName);
            if (descriptor == null) {
            	addError(req, "Can only run flow instance on one job.");
                return "";
            }
            else {
            	return req.getContextPath() + "/flow?job_id=" + jobName;
            }
        }
        else {
	        for(String job: jobNames) {
	            if(hasParam(req, "schedule")) {
	                int hour = getIntParam(req, "hour");
	                int minutes = getIntParam(req, "minutes");
	                boolean isPm = getParam(req, "am_pm").equalsIgnoreCase("pm");
	                String scheduledDate = req.getParameter("date");
	                DateTime day = null;
	                if(scheduledDate == null || scheduledDate.trim().length() == 0) {
	                	day = new LocalDateTime().toDateTime();
	                } else {
		                try {
		                	day = DateTimeFormat.forPattern("MM-dd-yyyy").parseDateTime(scheduledDate);
		                } catch(IllegalArgumentException e) {
		                	addError(req, "Invalid date: '" + scheduledDate + "'");
		                	return "";
		                }
	                }
	
	                ReadablePeriod thePeriod = null;
	                if(hasParam(req, "is_recurring"))
	                    thePeriod = parsePeriod(req);
	
	                if(isPm && hour < 12)
	                    hour += 12;
	                hour %= 24;
	
	                app.getScheduleManager().schedule(job,
                            day.withHourOfDay(hour)
                            .withMinuteOfHour(minutes)
                            .withSecondOfMinute(0),
                         thePeriod,
                         false);

	                addMessage(req, job + " scheduled.");
	            } else if(hasParam(req, "run_now")) {
	                boolean ignoreDeps = !hasParam(req, "include_deps");
	                try {
	                	app.getJobExecutorManager().execute(job, ignoreDeps);
	                }
	                catch (JobExecutionException e) {
	                	addError(req, e.getMessage());	
	                	return "";
	                }
	                addMessage(req, "Running " + job);
	            }
	            else {
	                addError(req, "Neither run_now nor schedule param is set.");
	            }
	        }
	        return "";
        }

    }

    private ReadablePeriod parsePeriod(HttpServletRequest req) throws ServletException {
        int period = getIntParam(req, "period");
        String periodUnits = getParam(req, "period_units");
        if("d".equals(periodUnits))
            return Days.days(period);
        else if("h".equals(periodUnits))
            return Hours.hours(period);
        else if("m".equals(periodUnits))
            return Minutes.minutes(period);
        else if("s".equals(periodUnits))
            return Seconds.seconds(period);
        else
            throw new ServletException("Unknown period unit: " + periodUnits);
    }

}
