package azkaban.web.pages;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import azkaban.flow.ComposedExecutableFlow;
import azkaban.flow.ExecutableFlow;
import azkaban.flow.IndividualJobExecutableFlow;
import azkaban.flow.MultipleDependencyExecutableFlow;
import azkaban.flow.WrappingExecutableFlow;
import azkaban.jobs.Status;
import azkaban.jobs.JobExecutorManager.ExecutingJobAndInstance;
import azkaban.util.json.JSONUtils;

/**
 * Common helper class for common cross page functionality
 * 
 * @author rpark
 *
 */
public class ExecutingJobUtils {
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormat.forPattern("MM-dd-yyyy HH:mm:ss");
	
    public ExecutingJobUtils() {
    	
    }
    
    public String getExecutableFlowJSON(Collection<ExecutableFlow> elements) {
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
    
    
    public String getExecutableJobAndInstanceJSON(Collection<ExecutingJobAndInstance> elements) {
    	if(elements.isEmpty()) {
    		return "[]";
    	}
    	
    	StringBuffer buffer = new StringBuffer();
    	buffer.append("[");
    	for (ExecutingJobAndInstance flow : elements ) {
    		buffer.append((JSONUtils.toJSONString(getExecutableElement(flow.getExecutableFlow()))));
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