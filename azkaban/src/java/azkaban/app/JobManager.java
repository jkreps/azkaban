package azkaban.app;

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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import azkaban.flow.manager.FlowManager;
import azkaban.flow.Flows;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.collect.ImmutableSet;

import azkaban.jobcontrol.impl.jobs.JobGraph;
import azkaban.jobcontrol.impl.jobs.ResourceThrottledJob;
import azkaban.jobcontrol.impl.jobs.RetryingJob;
import azkaban.jobcontrol.impl.jobs.locks.GroupLock;
import azkaban.jobcontrol.impl.jobs.locks.JobLock;
import azkaban.jobcontrol.impl.jobs.locks.NamedPermitManager;
import azkaban.jobcontrol.impl.jobs.locks.PermitLock;
import azkaban.jobcontrol.impl.jobs.locks.ReadWriteLockManager;

import azkaban.common.jobs.Job;
import azkaban.common.utils.Props;
import azkaban.common.utils.UndefinedPropertyException;
import azkaban.common.utils.Utils;

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

    private final String _logDir;
    private final Props _defaultProps;
    private final List<File> _jobDirs;
    private final ClassLoader _baseClassLoader;

    private final NamedPermitManager _permitManager;
    private final ReadWriteLockManager _readWriteLockManager;

    private static Logger logger = Logger.getLogger(JobManager.class);
    private JobWrapperFactory _factory;

    private volatile FlowManager manager;
    private final ScheduledExecutorService cacheReloader = Executors.newSingleThreadScheduledExecutor();
    private final AtomicReference<Map<String, JobDescriptor>> noDependencyDescriptorCache;

    public JobManager(String logDir, Props defaultProps, List<File> jobDirs, ClassLoader classLoader) {
        this(logDir, defaultProps, jobDirs, classLoader, Integer.MAX_VALUE);
    }

    public JobManager(String logDir,
                      Props defaultProps,
                      List<File> jobDirs,
                      ClassLoader classLoader,
                      int totalPermits) {
        this._logDir = logDir;
        this._defaultProps = defaultProps;
        this._jobDirs = jobDirs;
        this._baseClassLoader = classLoader;
        this._permitManager = new NamedPermitManager();
        this._permitManager.createNamedPermit("default", totalPermits);
        this._readWriteLockManager = new ReadWriteLockManager();

        _factory = new JobWrapperFactory();
        _factory.registerJobExecutorType("java", JavaJob.class);
        _factory.registerJobExecutorType("command", ProcessJob.class);

        noDependencyDescriptorCache = new AtomicReference<Map<String, JobDescriptor>>();
        noDependencyDescriptorCache.set(loadJobDescriptors(new Props(), Collections.<File, File>emptyMap(), true));

        cacheReloader.scheduleAtFixedRate(
                new Runnable()
                {
                    @Override
                    public void run()
                    {
                        try {
                            noDependencyDescriptorCache.set(
                                    loadJobDescriptors(new Props(), Collections.<File, File>emptyMap(), true)
                            );
                            if (manager != null) {
                                manager.reload();
                            }
                        }
                        catch (Throwable t) {
                            logger.error("Got an exception while updating jobDescriptor cache.  Not good.", t);
                        }
                    }
                },
                5,
                5,
                TimeUnit.MINUTES
        );
    }

    public Job loadJob(String jobName, boolean ignoreDep) {
        return loadJob(jobName, new Props(), ignoreDep);
    }

    public void validateJob(String jobName, boolean ignoreDep) {
        // for now just load the job and see if it blows up
        loadJob(jobName, ignoreDep);
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
                       descriptors,
                       new HashMap<String, Job>(),
                       ignoreDependencies,
                       new ConcurrentHashMap<Job, JobGraph.JobSTATUS>());
    }

    /*
     * Recursive inner method for loading a Job and its dependencies
     */
    private Job loadJob(JobDescriptor desc,
                        Map<String, JobDescriptor> descriptors,
                        Map<String, Job> loadedJobs,
                        boolean ignoreDependencies,
                        ConcurrentHashMap<Job, JobGraph.JobSTATUS> jobStatusMap) {
        String jobName = desc.getId();

        // Check if we have already loaded this job
        if(loadedJobs.containsKey(jobName))
            return loadedJobs.get(jobName);

        Job job = createBasicJob(desc);

        // wrap up job in retrying proxy if necessary
        if(desc.getRetries() > 0)
            job = new RetryingJob(job, desc.getRetries(), desc.getRetryBackoffMs());

        // Group Lock List
        ArrayList<JobLock> jobLocks = new ArrayList<JobLock>();

        // If this job requires work permits wrap it in a resource throttler
        if(desc.getNumRequiredPermits() > 0) {
            PermitLock permits = _permitManager.getNamedPermit("default",
                                                               desc.getNumRequiredPermits());
            if(permits == null) {
                throw new JobLoadException("Job " + desc.getId() + " requires non-existant default");
            } else if(permits.getDesiredNumPermits() > permits.getTotalNumberOfPermits()) {
                throw new JobLoadException("Job " + desc.getId() + " requires "
                                           + permits.getTotalNumberOfPermits()
                                           + " but azkaban only has a total of "
                                           + permits.getDesiredNumPermits()
                                           + " permits, so this job cannot ever run.");
            } else {
                jobLocks.add(permits);
            }
        }

        if(desc.getReadResourceLocks() != null) {
            List<String> readLocks = desc.getReadResourceLocks();
            for(String resource: readLocks) {
                jobLocks.add(_readWriteLockManager.getReadLock(resource));
            }
        }
        if(desc.getWriteResourceLocks() != null) {
            List<String> writeLocks = desc.getWriteResourceLocks();
            for(String resource: writeLocks) {
                jobLocks.add(_readWriteLockManager.getWriteLock(resource));
            }
        }

        if(jobLocks.size() > 0) {
            // Group lock
            GroupLock groupLock = new GroupLock(jobLocks);
            job = new ResourceThrottledJob(job, groupLock);
        }

        // wrap up job in logging proxy
        job = new LoggingJob(_logDir, job, job.getId());

        // wrap up job in JobGraph if necessary
        if(ignoreDependencies || !desc.hasDependencies()) {
            loadedJobs.put(jobName, job);
            return job;
        } else {
            JobGraph graphJob = new JobGraph(desc.getId(),
                                             desc.getProps(),
                                             jobStatusMap,
                                             descriptors);
            loadedJobs.put(jobName, graphJob);
            List<Job> dependencies = new ArrayList<Job>();
            for(JobDescriptor dep: desc.getDependencies())
                dependencies.add(loadJob(dep,
                                         descriptors,
                                         loadedJobs,
                                         ignoreDependencies,
                                         jobStatusMap));
            graphJob.addJob(job, dependencies);
            return graphJob;
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
                File runProps = new File(execDir, "run.properties");
                DateTime start = null;
                DateTime end = null;
                boolean succeeded = false;
                DateTime dirDate = JOB_EXEC_DATE_FORMAT.parseDateTime(execDir.getName());
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
                                           logFile));
            }
        }
        return execs;
    }

    public Map<String, JobDescriptor> loadJobDescriptors() {
        return loadJobDescriptors(null, new HashMap<File, File>(), false);
    }

    public Map<String, JobDescriptor> loadJobDescriptors(Props overrides,
                                                         Map<File, File> pathOverrides,
                                                         boolean ignoreDeps) {
        Map<String, JobDescriptor> cachedRetVal = noDependencyDescriptorCache.get();
        if ((pathOverrides == null || pathOverrides.isEmpty()) && ignoreDeps && cachedRetVal != null) { // can give cached response
            return cachedRetVal;           
        }

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
        if(!jobDir.exists() && jobDir.isDirectory())
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
        for(File f: currDir.listFiles()) {
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
                    jobs.put(name, new JobDescriptor(name, jobPath, jobProps, loader));
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
                        urls.add(f.toURL());
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
     * Instantiate the given job
     * 
     * @param dep The job descriptor
     * @return The instantiated job
     */
    private Job createBasicJob(JobDescriptor dep) {
        return _factory.getJobExecutor(dep);
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
        File basePath = this._jobDirs.get(0);

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

    private void updateFlowManager()
    {
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
}
