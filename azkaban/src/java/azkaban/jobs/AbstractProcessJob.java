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
package azkaban.jobs;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.commons.fileupload.util.Streams;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import azkaban.app.JobDescriptor;
import azkaban.app.PropsUtils;
import azkaban.common.jobs.AbstractJob;
import azkaban.common.utils.Props;
import azkaban.util.JSONToJava;

/**
 * A revised process-based job
 * 
 * @author jkreps
 * 
 */
public abstract class AbstractProcessJob extends AbstractJob {

    private static final Logger log = Logger
            .getLogger(AbstractProcessJob.class);

    public static final String ENV_PREFIX = "env.";
    public static final String ENV_PREFIX_UCASE = "ENV.";
    public static final String WORKING_DIR = "working.dir";
    public static final String JOB_PROP_ENV = "JOB_PROP_FILE";
    public static final String JOB_NAME_ENV = "JOB_NAME";
    public static final String JOB_OUTPUT_PROP_FILE = "JOB_OUTPUT_PROP_FILE";

    private static final JSONToJava jsonToJava = new JSONToJava();

    protected final String _jobPath;

    protected final JobDescriptor _descriptor;
    protected volatile Props _props;

    protected String _cwd;

    private volatile Props generatedPropeties;

    protected AbstractProcessJob(final JobDescriptor descriptor) {
        super(descriptor.getId());

        _props = descriptor.getProps();
        _jobPath = descriptor.getFullPath();

        _descriptor = descriptor;
        _cwd = getWorkingDirectory();
    }

    public JobDescriptor getJobDescriptor() {
        return _descriptor;
    }

    public Props getProps() {
        return _props;
    }

    public String getJobPath() {
        return _jobPath;
    }

    protected void resolveProps() {
        _props = PropsUtils.resolveProps(_props);
    }

    @Override
    public Props getJobGeneratedProperties() {
        return generatedPropeties;
    }

    /**
     * initialize temporary and final property file
     * 
     * @return {tmpPropFile, outputPropFile}
     */
    public File[] initPropsFiles() {
        // Create properties file with additionally all input generated
        // properties.
        File[] files = new File[2];
        files[0] = createFlattenedPropsFile(_cwd);

        _props.put(ENV_PREFIX + JOB_PROP_ENV, files[0].getAbsolutePath());
        _props.put(ENV_PREFIX + JOB_NAME_ENV, getId());

        files[1] = createOutputPropsFile(getId(), _cwd);
        _props.put(ENV_PREFIX + JOB_OUTPUT_PROP_FILE,
                files[1].getAbsolutePath());

        return files;
    }

    public String getCwd() {
        return _cwd;
    }

    public Map<String, String> getEnvironmentVariables() {
        Props props = getProps();
        Map<String, String> envMap = props.getMapByPrefix(ENV_PREFIX);
        envMap.putAll(props.getMapByPrefix(ENV_PREFIX_UCASE));
        return envMap;
    }

    public String getWorkingDirectory() {
        return getProps()
                .getString(WORKING_DIR, new File(_jobPath).getParent());
    }

    public Props loadOutputFileProps(final File outputPropertiesFile) {
        InputStream reader = null;
        try {
            System.err.println("output properties file="
                    + outputPropertiesFile.getAbsolutePath());
            reader = new BufferedInputStream(new FileInputStream(
                    outputPropertiesFile));

            Props outputProps = new Props();
            final String content = Streams.asString(reader).trim();

            if (!content.isEmpty()) {
                Map<String, Object> propMap = jsonToJava.apply(new JSONObject(
                        content));

                for (Map.Entry<String, Object> entry : propMap.entrySet()) {
                    outputProps
                            .put(entry.getKey(), entry.getValue().toString());
                }
            }
            return outputProps;
        } catch (FileNotFoundException e) {
            log.info(String.format(
                    "File[%s] wasn't found, returning empty props.",
                    outputPropertiesFile));
            return new Props();
        } catch (Exception e) {
            log.error(
                    "Exception thrown when trying to load output file props.  Returning empty Props instead of failing.  Is this really the best thing to do?",
                    e);
            return new Props();
        } finally {
            IOUtils.closeQuietly(reader);
        }
    }

    public File createFlattenedPropsFile(final String workingDir) {
        File directory = new File(workingDir);
        File tempFile = null;
        try {
            tempFile = File.createTempFile(getId() + "_", "_tmp", directory);
            _props.storeFlattened(tempFile);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create temp property file ",
                    e);
        }

        return tempFile;
    }

    public static File createOutputPropsFile(final String id,
            final String workingDir) {
        System.err.println("cwd=" + workingDir);

        File directory = new File(workingDir);
        File tempFile = null;
        try {
            tempFile = File.createTempFile(id + "_output_", "_tmp", directory);
        } catch (IOException e) {
            System.err
                    .println("Failed to create temp output property file :\n");
            e.printStackTrace(System.err);
            throw new RuntimeException(
                    "Failed to create temp output property file ", e);
        }
        return tempFile;
    }

    public void generateProperties(final File outputFile) {
        generatedPropeties = loadOutputFileProps(outputFile);
    }

}
