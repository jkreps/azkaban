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

import azkaban.common.jobs.Job;
import azkaban.common.utils.Props;
import azkaban.common.utils.UndefinedPropertyException;
import azkaban.common.utils.Utils;
import azkaban.flow.FlowManager;
import azkaban.jobs.JobExecution;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The JobManager is responsible for managing the Jobs (duh) and the
 * JobDescriptors.
 * 
 * JobManager can load JobDescriptors, create Jobs from JobDescriptors, etc.
 * 
 * @author jkreps
 * 
 */
public class JobManager {

    private static final String JOB_SUFFIX = ".job";
    private static final DateTimeFormatter JOB_EXEC_DATE_FORMAT = DateTimeFormat.forPattern("MM-dd-yyyy.HH.mm.ss.SSS");
    private static final Set<String> EXCLUDE_PATHS = ImmutableSet.of("__MACOSX");
    private static final Comparator<JobExecution> JOB_EXEC_COMPARATOR = new Comparator<JobExecution>() {

        public int compare(JobExecution e1, JobExecution e2) {
            long ended1 = e1.getStarted() == null ? Long.MIN_VALUE : e1.getStarted().getMillis();
            long ended2 = e2.getStarted() == null ? Long.MIN_VALUE : e2.getStarted().getMillis();
            return (int) Math.signum(ended2 - ended1);
        }
    };

    private final JobWrappingFactory _factory;
    private final String _logDir;
    private final Props _defaultProps;
    private final List<File> _jobDirs;
    private final ClassLoader _baseClassLoader;

    private static Logger logger = Logger.getLogger(JobManager.class);

    private volatile FlowManager manager;
    private final AtomicReference<Map<String, JobDescriptor>> jobDescriptorCache =
            new AtomicReference<Map<String, JobDescriptor>>(Collections.<String, JobDescriptor>emptyMap());

    public JobManager(
            final JobWrappingFactory factory,
            final String logDir,
            final Props defaultProps,
            final List<File> jobDirs,
            final ClassLoader classLoader
    ) {
        this._factory = factory;
        this._logDir = logDir;
        this._defaultProps = defaultProps;
        this._jobDirs = jobDirs;
        this._baseClassLoader = classLoader;
    }

    public Job loadJob(String jobName, boolean ignoreDep) {
        return loadJob(jobName, new Props(), ignoreDep);
    }

    public void validateJob(String jobName) {
        // for now just load the job and see if it blows up
        loadJob(jobName, true);
    }

    /**
     * Load a job instance corresponding to the given job name
     * 
     * @param jobName The name of the job
     * @return An instance of the job
     */
    public Job loadJob(String jobName, Props overrides, boolean ignoreDependencies) {
        Map<String, JobDescriptor> descriptors = loadJobDescriptors(overrides,
                                                                    new HashMap<File, File>(),
                                                                    ignoreDependencies);
        JobDescriptor desc = descriptors.get(jobName);
        if(desc == null)
            throw new JobLoadException("No job descriptor found for job '" + jobName + "'.");
        return loadJob(desc,
                       new HashMap<String, Job>(),
                       ignoreDependencies);
    }

    /*
     * Recursive inner method for loading a Job and its dependencies
     */
    private Job loadJob(JobDescriptor desc,
                        Map<String, Job> loadedJobs,
                        boolean ignoreDependencies) {
        String jobName = desc.getId();

        // Check if we have already loaded this job
        if(loadedJobs.containsKey(jobName))
            return loadedJobs.get(jobName);

        Job job = _factory.apply(desc);

        if(ignoreDependencies || !desc.hasDependencies()) {
            loadedJobs.put(jobName, job);
            return job;
        } else {
            throw new RuntimeException("No longer support the loading of jobs with dependencies.  Use FlowManager instead.");
        }
    }

    /**
     * Get all the JobDescriptors that are not dependencies for any other job.
     * 
     * @return The set of all root JobDescriptors
     */
    public Set<JobDescriptor> getRootJobDescriptors(Map<String, JobDescriptor> jobDescriptors) {
        Set<JobDescriptor> s = new HashSet<JobDescriptor>();
        s.addAll(jobDescriptors.values());
        for(JobDescriptor desc: jobDescriptors.values())
            for(JobDescriptor dep: desc.getDependencies())
                s.remove(dep);
        return s;
    }

    public List<JobExecution> loadRecentJobExecutions(int count) throws IOException {
        // load job executions for all jobs
        File logDir = new File(_logDir);
        File[] jobDirs = logDir.listFiles();
        if(jobDirs == null)
            return Collections.emptyList();

        List<JobExecution> execs = new ArrayList<JobExecution>();
        for(File jobDir: jobDirs) {
            List<JobExecution> found = loadJobExecutions(jobDir.getName(), jobDir);
            if(found.size() > count)
                found.subList(count, found.size()).clear();
            execs.addAll(found);
        }

        Collections.sort(execs, JOB_EXEC_COMPARATOR);
        if(execs.size() > count)
            execs.subList(count, execs.size()).clear();
        return execs;
    }

    /**
     * Load all the past executions of the given job
     * 
     * @param jobName The name of the job
     * @return A list of past executions sorted from most to least recent
     * @throws IOException If loading fails
     */
    public List<JobExecution> loadJobExecutions(String jobName) throws IOException {
        File dir = new File(_logDir + File.separator + jobName);
        List<JobExecution> execs = loadJobExecutions(jobName, dir);

        // sort the executions from latest to oldest
        Collections.sort(execs, JOB_EXEC_COMPARATOR);
        return execs;
    }

    private List<JobExecution> loadJobExecutions(String jobName, File jobDir) throws IOException {
        List<JobExecution> execs = new ArrayList<JobExecution>();
        File[] files = jobDir.listFiles();
        if(files != null) {
            for(File execDir: files) {
                // Sometimes there are os or other maintenance files that should be skipped.
                if (!execDir.isDirectory() || execDir.getName().startsWith(".")) {
                    continue;
                }
                File runProps = new File(execDir, "run.properties");
                DateTime start = null;
                DateTime end = null;
                boolean succeeded = false;
                DateTime dirDate = null;
                try {
                    dirDate = JOB_EXEC_DATE_FORMAT.parseDateTime(execDir.getName());
                } catch (Exception e) {
                    logger.info("Ignoring unknown directory found in logs:" + execDir.getAbsolutePath(), e);
                    continue;
                }
                if(runProps.canRead()) {
                    Props props = new Props(null, runProps.getAbsolutePath());
                    start = new DateTime(props.getLong("start"));
                    end = new DateTime(props.getLong("end"));
                    succeeded = props.getBoolean("succeeded");
                }
                String logFile = jobName + File.separator + execDir.getName() + File.separator
                                 + jobName + "." + execDir.getName() + ".log";
                execs.add(new JobExecution(jobName,
                                           start == null ? dirDate : start,
                                           end,
                                           succeeded,
                                           false,
                                           logFile));
            }
        }
        return execs;
    }

    public JobDescriptor getJobDescriptor(String name)
    {
        return jobDescriptorCache.get().get(name);
    }

    public Map<String, JobDescriptor> loadJobDescriptors() {
        return loadJobDescriptors(null, new HashMap<File, File>(), false);
    }

    public Map<String, JobDescriptor> loadJobDescriptors(Props overrides,
                                                         Map<File, File> pathOverrides,
                                                         boolean ignoreDeps) {
        Map<String, JobDescriptor> descriptors = new HashMap<String, JobDescriptor>();
        for(File file: _jobDirs) {
            Map<String, JobDescriptor> d = loadJobDescriptors(file,
                                                              overrides,
                                                              pathOverrides,
                                                              ignoreDeps);
            for(Map.Entry<String, JobDescriptor> entry: d.entrySet()) {
                if(descriptors.containsKey(entry.getKey()))
                    throw new IllegalStateException("Job " + entry.getKey() + " already exists.");
                descriptors.put(entry.getKey(), entry.getValue());
            }
        }
        return descriptors;
    }

    /**
     * Load all the JobDescriptors in the given directory, replacing all
     * existing descriptors.
     * 
     * @param jobDir The directory to load from
     * @param propsOverrides Properties that override all other properties
     */
    protected Map<String, JobDescriptor> loadJobDescriptors(File jobDir,
                                                            Props propsOverrides,
                                                            Map<File, File> pathOverrides,
                                                            boolean ignoreDeps) {
        if(!jobDir.exists() || !jobDir.isDirectory())
            throw new AppConfigurationException(jobDir + " is not a readable directory.");
        Map<String, JobDescriptor> m = new HashMap<String, JobDescriptor>();
        loadJobDescriptorsWithoutDependencies(m,
                                              jobDir,
                                              jobDir,
                                              _defaultProps,
                                              propsOverrides,
                                              pathOverrides,
                                              _baseClassLoader);

        if(pathOverrides != null) {
            Props dirProps = loadLocalNonJobProps(jobDir, _defaultProps);
            for(Map.Entry<File, File> override: pathOverrides.entrySet()) {
                loadJobDescriptorsWithoutDependencies(m,
                                                      jobDir,
                                                      override.getValue(),
                                                      dirProps,
                                                      propsOverrides,
                                                      pathOverrides,
                                                      _baseClassLoader);
            }

        }
        if(!ignoreDeps)
            addDependencies(m);
        return m;
    }

    /*
     * Recursive helper to load job descriptors from the filesystem
     */
    private void loadJobDescriptorsWithoutDependencies(Map<String, JobDescriptor> jobs,
                                                       File baseDir,
                                                       File currDir,
                                                       Props defaultProps,
                                                       Props overrides,
                                                       Map<File, File> pathOverrides,
                                                       ClassLoader parentClassLoader) {
        // first load additional props defined in this directory
        Props dirProps = loadLocalNonJobProps(currDir, defaultProps);

        // apply overrides
        dirProps = new Props(dirProps, overrides);

        ClassLoader loader = createClassLoaderForDir(parentClassLoader, currDir);

        // now load any files defined in this directory
        File[] status = currDir.listFiles();
        for(File f: status) {
            if(EXCLUDE_PATHS.contains(f.getName()) || f.getName().startsWith(".")) {
                // ignore common files
                continue;
            } else if(f.isFile() && f.getName().endsWith(JOB_SUFFIX)) {
                String name = f.getName().substring(0, f.getName().length() - JOB_SUFFIX.length());
                String jobPath = getJobPath(baseDir, currDir);
                if(jobs.containsKey(name))
                    throw new JobLoadException(
                            String.format(
                                    "Job at path [%s] has duplicate name[%s] as another job[%s].",
                                    jobPath,
                                    name,
                                    jobs.get(name)
                            )
                    );

                logger.debug("Loading job '" + name + "' with path " + jobPath);
                try {
                    Props jobProps = new Props(dirProps, f.getAbsolutePath());
                    jobs.put(name, new JobDescriptor(name, jobPath, f.getPath(), jobProps, loader));
                } catch(Exception e) {
                    throw new JobLoadException("Failed to create Job '" + name + "': "
                                               + e.getLocalizedMessage(), e);
                }
            } else if(f.isDirectory()) {
                // path overrides allow us to mask out certain directories for
                // verification purposes
                if(pathOverrides != null && pathOverrides.containsKey(f))
                    continue;
                else
                    loadJobDescriptorsWithoutDependencies(jobs,
                                                          baseDir,
                                                          f,
                                                          dirProps,
                                                          overrides,
                                                          pathOverrides,
                                                          loader);
            } else {
                logger.debug("Ignoring unknown file " + f.getAbsolutePath());
            }
        }
    }

    private String getJobPath(File baseDir, File currDir) {
        try {
            return currDir.getCanonicalPath().substring(baseDir.getCanonicalPath().length());
        } catch(IOException e) {
            throw new JobLoadException("Error while cannonicalizing job path.", e);
        }
    }

    /**
     * Create a classloader that has all the jars in the local directory on the
     * classpath
     * 
     * @param parentClassLoader The parent classloader
     * @param dir The directory to look for jars in
     * @return The classloader
     */
    private ClassLoader createClassLoaderForDir(ClassLoader parentClassLoader, File dir) {
        ArrayList<URL> urls = new ArrayList<URL>();
        File[] files = dir.listFiles();
        if(files == null) {
            return parentClassLoader;
        } else {
            for(File f: files) {
                if(f.getName().endsWith(".jar")) {
                    try {
                        logger.debug("Adding jar " + f.getName() + " to the classpath");
                        urls.add(f.toURI().toURL());
                    } catch(MalformedURLException e) {
                        throw new JobLoadException(e);
                    }
                }
            }
            URL[] jars = urls.toArray(new URL[urls.size()]);
            if(jars.length == 0)
                return parentClassLoader;
            else
                return new URLClassLoader(jars, parentClassLoader);
        }
    }

    /**
     * Add dependencies to all the jobs in the given map
     * 
     * @param jobs
     */
    private void addDependencies(Map<String, JobDescriptor> jobs) {
        // add all dependencies
        for(JobDescriptor job: jobs.values()) {
            List<String> dependencies = job.getProps().getStringList("dependencies",
                                                                     new ArrayList<String>());
            for(String dep: dependencies) {
                String name = dep.trim();
                if(Utils.isNullOrEmpty(name))
                    continue;

                if(jobs.containsKey(name))
                    job.addDependency(jobs.get(name));
                else
                    throw new AppConfigurationException("Job '"
                                                        + job.getId()
                                                        + "' depends on job '"
                                                        + name
                                                        + "' which does not exist (check the spelling of the job name!).");
            }
        }

        // check for cycles
    }

    /**
     * Load all files that are not jobs from the given directory as Props with
     * the given parent
     * 
     * @param dir The directory to load from
     * @param parent The parent Props instance
     * @return The loaded Props
     */
    private Props loadLocalNonJobProps(File dir, Props parent) {
        if(!dir.isDirectory())
            throw new JobLoadException("Directory '" + dir + "' is not a valid directory path!");
        else if(!dir.canRead())
            throw new JobLoadException(dir + " is not a readable directory!");

        try {
            Props props = new Props(parent);
            for(File f: dir.listFiles()) {
                try {
                    String name = f.getName();
                    if(name.endsWith(".schema") || name.endsWith(".properties")) {
                        logger.debug("Loading properties from " + f.getAbsolutePath());
                        props.putLocal(new Props(null, f.getAbsolutePath()));
                    }
                } catch(UndefinedPropertyException e) {
                    throw new JobLoadException("Undefined property while loading properties in '"
                                               + f + "'.", e);
                }
            }
            return props;
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void verifyPathValidity(File localPath, File destPath) {
        Map<File, File> m = new HashMap<File, File>();
        m.put(destPath, localPath);
        // verify job load

        loadJobDescriptors(null, m, false);

    }

    public void setFlowManager(FlowManager theManager)
    {
        synchronized (this) {
            if (manager == null) {
                manager = theManager;
            }
            else {
                throw new IllegalStateException("Can only set the FlowManager on JobManager onces.");
            }
        }

        updateFlowManager();
    }

    public void updateFlowManager()
    {
        jobDescriptorCache.set(loadJobDescriptors());
        manager.reload();
    }

    public void deployJobDir(String localPath, String destPath) {
        File targetPath = new File(this._jobDirs.get(0), destPath);
        verifyPathValidity(new File(localPath), targetPath);
        if(targetPath.exists()) {
            logger.info("Undeploying job at " + destPath);
            try {
                FileUtils.deleteDirectory(targetPath);
            } catch(Exception e) {
                throw new RuntimeException(e);
            }
        }

        if(!targetPath.mkdirs())
            throw new RuntimeException("Failed to create target directory " + targetPath);

        File currPath = new File(localPath);
        if(currPath.renameTo(targetPath))
            logger.info(destPath + " deployed.");
        else
            throw new RuntimeException("Deploy failed because " + currPath
                                       + " could not be moved to " + destPath);

        updateFlowManager();
    }

    public void deployJob(String jobName, String path, Props props) {
        File jobPath = new File(_jobDirs.get(0), path);
        if(jobPath.exists()) {
            if(!jobPath.isDirectory() || !jobPath.canWrite())
                throw new JobDeploymentException(jobPath
                                                 + " is not a directory or does not have write permission.");
        } else {
            logger.debug("Creating job directory " + jobPath);
            jobPath.mkdirs();
        }

        // @TODO validate and prevent addition of changes y
        File jobFile = new File(jobPath, jobName + ".job");
        jobFile.delete();
        try {
            props.storeLocal(jobFile);
        } catch(IOException e) {
            throw new RuntimeException("Error deploying job " + jobName);
        }
        logger.info("Deployed job " + jobName + " to path " + path);

        updateFlowManager();
    }

    /**
     * Load most recent execution of a job
     * 
     * @param jobId
     * @return
     */
    public JobExecution loadMostRecentJobExecution(String jobId) {

        JobExecution ret = null;
        try {
            List<JobExecution> list = this.loadJobExecutions(jobId);

            if(list == null)
                logger.warn("Job execution list for " + jobId + " is null");
            else if(list.isEmpty())
                logger.warn("Empty execution for job " + jobId);
            else
                ret = list.get(0); // get the most recent job execution
        } catch(IOException e) {
            logger.error("Error in loading job execution list: \n" + Utils.stackTrace(e) + "\n");
        }
        return ret;
    }

    /**
     * Get the absolute path of the log directory
     * 
     * @return
     */
    public String getLogDir() {
        return _logDir;
    }
}
