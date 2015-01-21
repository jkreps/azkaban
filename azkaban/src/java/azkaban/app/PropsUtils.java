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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import azkaban.common.utils.Props;
import azkaban.common.utils.UndefinedPropertyException;
import azkaban.flow.ExecutableFlow;
import org.joda.time.DateTime;

public class PropsUtils {

    /**
     * Load job schedules from the given directories ] * @param dir The
     * directory to look in
     *
     * @param suffixes File suffixes to load
     * @return The loaded set of schedules
     */
    public static Props loadPropsInDir(File dir, String... suffixes) {
        return loadPropsInDir(null, dir, suffixes);
    }

    /**
     * Load job schedules from the given directories
     *
     * @param parent The parent properties for these properties
     * @param dir The directory to look in
     * @param suffixes File suffixes to load
     * @return The loaded set of schedules
     */
    public static Props loadPropsInDir(Props parent, File dir, String... suffixes) {
        try {
            Props props = new Props(parent);
            File[] files = dir.listFiles();
            if(files != null) {
                for(File f: files) {
                    if(f.isFile() && endsWith(f, suffixes)) {
                        props.putAll(new Props(null, f.getAbsolutePath()));
                    }
                }
            }
            return props;
        } catch(IOException e) {
            throw new RuntimeException("Error loading properties.", e);
        }
    }

    /**
     * Load job schedules from the given directories
     *
     * @param dirs The directories to check for properties
     * @param suffixes The suffixes to load
     * @return The properties
     */
    public static Props loadPropsInDirs(List<File> dirs, String... suffixes) {
        Props props = new Props();
        for(File dir: dirs) {
            props.putLocal(loadPropsInDir(dir, suffixes));
        }
        return props;
    }

    /**
     * Load properties from the given path
     *
     * @param jobPath The path to load from
     * @param props The parent properties for loaded properties
     * @param suffixes The suffixes of files to load
     */
    public static void loadPropsBySuffix(File jobPath, Props props, String... suffixes) {
        try {
            if(jobPath.isDirectory()) {
                File[] files = jobPath.listFiles();
                if(files != null) {
                    for(File file: files)
                        loadPropsBySuffix(file, props, suffixes);
                }
            } else if(endsWith(jobPath, suffixes)) {
                props.putAll(new Props(null, jobPath.getAbsolutePath()));
            }
        } catch(IOException e) {
            throw new RuntimeException("Error loading schedule properties.", e);
        }
    }

    public static boolean endsWith(File file, String... suffixes) {
        for(String suffix: suffixes)
            if(file.getName().endsWith(suffix))
                return true;
        return false;
    }

    private static final Pattern VARIABLE_PATTERN = Pattern.compile("\\$\\{([a-zA-Z_.0-9]+)\\}");

    public static Props resolveProps(Props props) {
    	Props resolvedProps = new Props();

    	for(String key : props.getKeySet()) {
	        StringBuffer replaced = new StringBuffer();
	        String value = props.get(key);
	        Matcher matcher = VARIABLE_PATTERN.matcher(value);
	        while(matcher.find()) {
	            String variableName = matcher.group(1);

	            if (variableName.equals(key)) {
	                throw new IllegalArgumentException(
	                        String.format("Circular property definition starting from property[%s]", key)
	                );
	            }

	            String replacement = props.get(variableName);
	            if(replacement == null)
	                throw new UndefinedPropertyException("Could not find variable substitution for variable '"
	                                                     + variableName + "' in key '" + key + "'.");

	            replacement = replacement.replaceAll("\\\\", "\\\\\\\\");
	            replacement = replacement.replaceAll("\\$", "\\\\\\$");

	            matcher.appendReplacement(replaced, replacement);
	            matcher.appendTail(replaced);

	            value = replaced.toString();
	            replaced = new StringBuffer();
	            matcher = VARIABLE_PATTERN.matcher(value);
	        }
	        matcher.appendTail(replaced);
	        resolvedProps.put(key, replaced.toString());
    	}

    	return resolvedProps;
    }

    public static Props produceParentProperties(final ExecutableFlow flow) {
        Props parentProps = new Props();

        parentProps.put("azkaban.flow.id", flow.getId());
        parentProps.put("azkaban.flow.uuid", UUID.randomUUID().toString());

        DateTime loadTime = new DateTime();

        parentProps.put("azkaban.flow.start.timestamp", loadTime.toString());
        parentProps.put("azkaban.flow.start.year", loadTime.toString("yyyy"));
        parentProps.put("azkaban.flow.start.month", loadTime.toString("MM"));
        parentProps.put("azkaban.flow.start.day", loadTime.toString("dd"));
        parentProps.put("azkaban.flow.start.hour", loadTime.toString("HH"));
        parentProps.put("azkaban.flow.start.minute", loadTime.toString("mm"));
        parentProps.put("azkaban.flow.start.seconds", loadTime.toString("ss"));
        parentProps.put("azkaban.flow.start.milliseconds", loadTime.toString("SSS"));
        parentProps.put("azkaban.flow.start.timezone", loadTime.toString("ZZZZ"));
        parentProps.put("azkaban.flow.start.epoch.milliseconds", Long.toString(loadTime.getMillis()));
        return parentProps;
    }
}
