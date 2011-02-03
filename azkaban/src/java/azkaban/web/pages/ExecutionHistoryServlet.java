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

import azkaban.common.web.Page;
import azkaban.flow.ComposedExecutableFlow;
import azkaban.flow.ExecutableFlow;
import azkaban.flow.FlowExecutionHolder;
import azkaban.flow.FlowManager;
import azkaban.flow.Flows;
import azkaban.flow.IndividualJobExecutableFlow;
import azkaban.flow.MultipleDependencyExecutableFlow;
import azkaban.flow.WrappingExecutableFlow;
import azkaban.jobs.JobExecutionException;
import azkaban.jobs.Status;
import azkaban.util.json.JSONUtils;
import azkaban.web.AbstractAzkabanServlet;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

public class ExecutionHistoryServlet extends AbstractAzkabanServlet {

    private static final long serialVersionUID = 1L;
    private DateTimeFormatter DATE_FORMATTER = DateTimeFormat.forPattern("MM-dd-yyyy HH:mm:ss");
    private DateTimeFormatter ZONE_FORMATTER = DateTimeFormat.forPattern("z");
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
            IOException {
        final FlowManager allFlows = this.getApplication().getAllFlows();

        /* set runtime properties from request and response*/
        super.setRuntimeProperties (req, resp);
        
        if (hasParam(req, "action")) {
            if ("restart".equals(getParam(req, "action")) && hasParam(req, "id")) {
                try {
                    long id = Long.parseLong(getParam(req, "id"));

                    final FlowExecutionHolder holder = allFlows.loadExecutableFlow(id);

                    if (holder == null) {
                        addMessage(req, String.format("Unknown flow with id[%s]", id));
                    }
                    else {
                        Flows.resetFailedFlows(holder.getFlow());
                        this.getApplication().getJobExecutorManager().execute(holder);

                        addMessage(req, String.format("Flow[%s] restarted.", id));
                    }
                }
                catch (NumberFormatException e) {
                    addMessage(req, String.format("Apparently [%s] is not a valid long.", getParam(req, "id")));
                }
                catch (JobExecutionException e) {
                	addMessage(req, "Error restarting " + getParam(req, "id") + ". " + e.getMessage());
                }
            }
        }

        long currMaxId = allFlows.getCurrMaxId();

        String beginParam = req.getParameter("begin");
        int begin = beginParam == null? 0 : Integer.parseInt(beginParam);
        
        String sizeParam = req.getParameter("size");
        int size = sizeParam == null? 20 : Integer.parseInt(sizeParam);

        List<ExecutableFlow> execs = new ArrayList<ExecutableFlow>(size);
        for (int i = begin; i < begin + size; i++) {
            final FlowExecutionHolder holder = allFlows.loadExecutableFlow(currMaxId - i);
            ExecutableFlow flow = null;
            if (holder != null)
                flow = holder.getFlow();

            if (flow != null)
                execs.add(flow);
        }


        Page page = newPage(req, resp, "azkaban/web/pages/execution_history.vm");
        page.add("executions", execs);
        page.add("begin", begin);
        page.add("size", size);
        page.add("currentTime", (new DateTime()).getMillis());
        page.add("jsonExecution", getExecutableJSON(execs));
        page.add("timezone", ZONE_FORMATTER.print(System.currentTimeMillis()));
        page.render();
    }
    
    private HashMap<String, Object> serializeExecutableFlow(ExecutableFlow flow) {
    	HashMap<String,Object> map = new HashMap<String,Object>();
    	map.put("name", flow.getName());
    	if (flow.getStartTime() != null) {
	    	map.put("starttime", flow.getStartTime().getMillis());
    		map.put("starttimestr", DATE_FORMATTER.print(flow.getStartTime().getMillis()));
    	}
    	if (flow.getEndTime() != null) {
    		map.put("endtime", flow.getEndTime().getMillis());
    		map.put("endtimestr", DATE_FORMATTER.print(flow.getEndTime().getMillis()));
    	}
    	map.put("status", getStringStatus(flow.getStatus()));
    	
    	return map;
    }
    
    private String getStringStatus(Status status) {
    	switch(status) {
    	case COMPLETED:
    		return "completed";
    	case FAILED:
    		return "failed";
    	case SUCCEEDED:
    		return "succeeded";
    	case RUNNING:
    		return "running";
    	case READY:
    		return "ready";
    	case IGNORED:
    	    return "disabled";
    	}
    
    	
    	return "normal";
    }
    
    private String getExecutableJSON(Collection<ExecutableFlow> elements) {
    	if (elements.isEmpty()) {
    		return "[]";
    	}
    	
    	StringBuffer buffer = new StringBuffer();
    	buffer.append("[");
    	for (ExecutableFlow flow : elements ) {
    		buffer.append((JSONUtils.toJSONString(getExecutableElement(flow))));
    		buffer.append(",");
    	}

		buffer.setCharAt(buffer.length() - 1, ']');
		return buffer.toString();
    }
    
    private HashMap<String, Object> getExecutableElement(ExecutableFlow flow) {
    	HashMap<String, HashMap<String, Object>> elementMap = new LinkedHashMap<String, HashMap<String, Object>>();
    	traverseFlow(elementMap, flow);
    	
    	ArrayList<HashMap<String, Object>> list = new ArrayList<HashMap<String, Object>>(elementMap.values());
    	Collections.sort(list, new TimeLineComparator());
    	
    	HashMap<String,Object> map = new HashMap<String,Object>();
    	map.put("id", flow.getId());
    	map.put("name", flow.getName());
    	map.put("status", getStringStatus(flow.getStatus()));
    	if (flow.getStartTime() != null) {
    		map.put("starttime", flow.getStartTime().getMillis());
    		map.put("starttimestr", DATE_FORMATTER.print(flow.getStartTime().getMillis()));
    	}
    	if (flow.getEndTime() != null) {
    		map.put("endtime", flow.getEndTime().getMillis());
    		map.put("endtimestr", DATE_FORMATTER.print(flow.getEndTime().getMillis()));
    	}
    	map.put("children", list);
    	
    	return map;
    }
    
    private void traverseFlow(HashMap<String, HashMap<String, Object>> jobList, ExecutableFlow flow) {
    	if (jobList.containsKey(flow.getName())) {
    		return;
    	}
    	if (flow instanceof IndividualJobExecutableFlow) {
    		jobList.put(flow.getName(), serializeExecutableFlow(flow));
    	}
    	else {
    		if (flow instanceof ComposedExecutableFlow) {
        		ExecutableFlow innerFlow = ((ComposedExecutableFlow) flow).getDepender();
        		traverseFlow(jobList, innerFlow);
    		}
    		else if (flow instanceof MultipleDependencyExecutableFlow) {
        		traverseFlow(jobList, ((MultipleDependencyExecutableFlow) flow).getActualFlow());
    		}
    		else if (flow instanceof WrappingExecutableFlow) {
        		traverseFlow(jobList, ((WrappingExecutableFlow) flow).getDelegateFlow());
    		}

    		for(ExecutableFlow childFlow : flow.getChildren()) {
    			traverseFlow(jobList, childFlow);
    		}
    	}

    }
    
    private class TimeLineComparator implements Comparator< HashMap<String,Object> > {
		@Override
		public int compare(HashMap<String,Object> arg0, HashMap<String,Object> arg1) {
			
			Long first = (Long)arg0.get("starttime");
			Long second = (Long)arg1.get("starttime");
			
			if (first == null) {
				if (second == null) {
					return 0;
				}
				
				return 1;
			}
			else if (second == null) {
				return -1;
			}
			
			int val = first.compareTo(second);
			
			if (val != 0) {
				return val;
			}

			first = (Long)arg0.get("endtime");
			second = (Long)arg1.get("endtime");
			
			if (first == null) {
				if (second == null) {
					return 0;
				}
				
				return 1;
			}
			else if (second == null) {
				return -1;
			}
			
			return first.compareTo(second);
		}
    	
    }

}
