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
import java.lang.management.ManagementFactory;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.TimeZone;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.Logger;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.log.Log4JLogChute;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;
import org.joda.time.DateTimeZone;

import azkaban.app.jmx.JobScheduler;
import azkaban.app.jmx.RefreshJobs;
import azkaban.common.jobs.Job;
import azkaban.common.utils.Props;
import azkaban.common.utils.Utils;
import azkaban.flow.CachingFlowManager;
import azkaban.flow.FlowManager;
import azkaban.flow.RefreshableFlowManager;
import azkaban.jobcontrol.impl.jobs.locks.NamedPermitManager;
import azkaban.jobcontrol.impl.jobs.locks.ReadWriteLockManager;

import azkaban.jobs.JobExecutorManager;
import azkaban.jobs.builtin.JavaJob;
import azkaban.jobs.builtin.JavaProcessJob;
import azkaban.jobs.builtin.NoopJob;
import azkaban.jobs.builtin.PigProcessJob;
import azkaban.jobs.builtin.ProcessJob;
import azkaban.jobs.builtin.PythonJob;
import azkaban.jobs.builtin.RubyJob;
import azkaban.jobs.builtin.ScriptJob;
import azkaban.monitor.MonitorImpl;
import azkaban.monitor.MonitorInterface;
import azkaban.monitor.MonitorInternalInterface;
import azkaban.scheduler.LocalFileScheduleLoader;
import azkaban.scheduler.ScheduleManager;
import azkaban.serialization.DefaultExecutableFlowSerializer;
import azkaban.serialization.ExecutableFlowSerializer;
import azkaban.serialization.FlowExecutionSerializer;
import azkaban.serialization.de.DefaultExecutableFlowDeserializer;
import azkaban.serialization.de.ExecutableFlowDeserializer;
import azkaban.serialization.de.FlowExecutionDeserializer;
import com.google.common.collect.ImmutableMap;

/**
 * Master application that runs everything
 *
 * This class will be loaded up either by running from the command line or via a
 * servlet context listener.
 *
 * @author jkreps
 *
 */
public class AzkabanApplication
{

    private static final Logger logger = Logger.getLogger(AzkabanApplication.class);
    private static final String INSTANCE_NAME = "instance.name";
    private static final String DEFAULT_TIMEZONE_ID = "default.timezone.id";
    
    private final String _instanceName;
    private final List<File> _jobDirs;
    private final File _logsDir;
    private final File _tempDir;
    private final VelocityEngine _velocityEngine;
    private final JobManager _jobManager;
    private final Mailman _mailer;
    private final ClassLoader _baseClassLoader;
    private final String _hdfsUrl;
    private final FlowManager _allFlows;
    private final MonitorImpl _monitor;
    
    private final JobExecutorManager _jobExecutorManager;
    private final ScheduleManager _schedulerManager;
    
    private MBeanServer mbeanServer;
    private ObjectName jobRefresherName;
    private ObjectName jobSchedulerName;
    
    public AzkabanApplication(final List<File> jobDirs, final File logDir, final File tempDir, final boolean enableDevMode) throws IOException {
        this._jobDirs = Utils.nonNull(jobDirs);
        this._logsDir = Utils.nonNull(logDir);
        this._tempDir = Utils.nonNull(tempDir);

        if(!this._logsDir.exists()) {
            this._logsDir.mkdirs();
        }

        if(!this._tempDir.exists()) {
            this._tempDir.mkdirs();
        }

        for(File jobDir: _jobDirs) {
            if(!jobDir.exists()) {
                logger.warn("Job directory " + jobDir + " does not exist. Creating.");
                jobDir.mkdirs();
            }
        }

        if(jobDirs.size() < 1) {
            throw new IllegalArgumentException("No job directory given.");
        }

        Props defaultProps = PropsUtils.loadPropsInDirs(_jobDirs, ".properties", ".schema");

        _baseClassLoader = getBaseClassloader();

        String defaultTimezoneID = defaultProps.getString(DEFAULT_TIMEZONE_ID, null);
        if (defaultTimezoneID != null) {
        	DateTimeZone.setDefault(DateTimeZone.forID(defaultTimezoneID));
        	TimeZone.setDefault(TimeZone.getTimeZone(defaultTimezoneID));
        }
        
        NamedPermitManager permitManager = getNamedPermitManager(defaultProps);
        JobWrappingFactory factory = new JobWrappingFactory(
                permitManager,
                new ReadWriteLockManager(),
                _logsDir.getAbsolutePath(),
                "java",
                new ImmutableMap.Builder<String, Class<? extends Job>>()
                 .put("java", JavaJob.class)
                 .put("command", ProcessJob.class)
                 .put("javaprocess", JavaProcessJob.class)
                 .put("pig", PigProcessJob.class)
                 .put("propertyPusher", NoopJob.class)
                 .put("python", PythonJob.class)
                 .put("ruby", RubyJob.class)
                 .put("script", ScriptJob.class).build());

        _hdfsUrl = defaultProps.getString("hdfs.instance.url", null);
        _jobManager = new JobManager(factory,
                                     _logsDir.getAbsolutePath(),
                                     defaultProps,
                                     _jobDirs,
                                     _baseClassLoader);

        _mailer = new Mailman(defaultProps.getString("mail.host", "localhost"),
                              defaultProps.getString("mail.user", ""),
                              defaultProps.getString("mail.password", ""));

        String failureEmail = defaultProps.getString("job.failure.email", null);
        String successEmail = defaultProps.getString("job.success.email", null);
        int schedulerThreads = defaultProps.getInt("scheduler.threads", 20);
        _instanceName = defaultProps.getString(INSTANCE_NAME, "");
        
        final File initialJobDir = _jobDirs.get(0);
        File schedule = getScheduleFile(defaultProps, initialJobDir);
        File backup = getBackupFile(defaultProps, initialJobDir);
        File executionsStorageDir = new File(
                defaultProps.getString("azkaban.executions.storage.dir", initialJobDir.getAbsolutePath() + "/executions")
        );
        if (! executionsStorageDir.exists()) {
            executionsStorageDir.mkdirs();
        }
        long lastExecutionId = getLastExecutionId(executionsStorageDir);
        logger.info(String.format("Using path[%s] for storing executions.", executionsStorageDir));
        logger.info(String.format("Last known execution id was [%s]", lastExecutionId));

        final ExecutableFlowSerializer flowSerializer = new DefaultExecutableFlowSerializer();
        final ExecutableFlowDeserializer flowDeserializer = new DefaultExecutableFlowDeserializer(_jobManager, factory);

        FlowExecutionSerializer flowExecutionSerializer = new FlowExecutionSerializer(flowSerializer);
        FlowExecutionDeserializer flowExecutionDeserializer = new FlowExecutionDeserializer(flowDeserializer);

        _monitor = MonitorImpl.getMonitor();

        _allFlows = new CachingFlowManager(
                new RefreshableFlowManager(
                        _jobManager,
                        flowExecutionSerializer,
                        flowExecutionDeserializer, 
                        executionsStorageDir,
                        lastExecutionId
                ),
                defaultProps.getInt("azkaban.flow.cache.size", 1000)
        );
        _jobManager.setFlowManager(_allFlows);

        _jobExecutorManager = new JobExecutorManager(
				        		_allFlows, 
				        		_jobManager, 
				        		_mailer, 
				        		failureEmail, 
				        		successEmail,
				        		schedulerThreads
				        	);
        
        this._schedulerManager = new ScheduleManager(_jobExecutorManager, new LocalFileScheduleLoader(schedule, backup));

        /* set predefined log url prefix 
        */
        String server_url = defaultProps.getString("server.url", null) ;
        if (server_url != null) {
            if (server_url.endsWith("/")) {
                _jobExecutorManager.setRuntimeProperty(AppCommon.DEFAULT_LOG_URL_PREFIX, server_url + "logs?file=" );
            } else {
                _jobExecutorManager.setRuntimeProperty(AppCommon.DEFAULT_LOG_URL_PREFIX, server_url + "/logs?file=" );
            }
        }

        this._velocityEngine = configureVelocityEngine(enableDevMode);
        
        configureMBeanServer();
    }

    private VelocityEngine configureVelocityEngine(final boolean devMode) {
        VelocityEngine engine = new VelocityEngine();
        engine.setProperty("resource.loader", "classpath");
        engine.setProperty("classpath.resource.loader.class",
                           ClasspathResourceLoader.class.getName());
        engine.setProperty("classpath.resource.loader.cache", !devMode);
        engine.setProperty("classpath.resource.loader.modificationCheckInterval", 5L);
        engine.setProperty("resource.manager.logwhenfound", false);
        engine.setProperty("input.encoding", "UTF-8");
        engine.setProperty("output.encoding", "UTF-8");
        engine.setProperty("directive.foreach.counter.name", "idx");
        engine.setProperty("directive.foreach.counter.initial.value", 0);
        //engine.setProperty("runtime.references.strict", true);
        engine.setProperty("directive.set.null.allowed", true);
        engine.setProperty("resource.manager.logwhenfound", false);
        engine.setProperty("velocimacro.permissions.allow.inline", true);
        engine.setProperty("velocimacro.library.autoreload", devMode);
        engine.setProperty("velocimacro.library", "/azkaban/web/macros.vm");
        engine.setProperty("velocimacro.permissions.allow.inline.to.replace.global", true);
        engine.setProperty("velocimacro.context.localscope", true);
        engine.setProperty("velocimacro.arguments.strict", true);
        engine.setProperty("runtime.log.invalid.references", devMode);
        engine.setProperty("runtime.log.logsystem.class", Log4JLogChute.class);
        engine.setProperty("runtime.log.logsystem.log4j.logger",
                           Logger.getLogger("org.apache.velocity.Logger"));
        engine.setProperty("parser.pool.size", 3);
        return engine;
    }
    
    private void configureMBeanServer() {
        logger.info("Registering MBeans...");
        mbeanServer = ManagementFactory.getPlatformMBeanServer();
        try {
            jobRefresherName = new ObjectName("azkaban.app.jmx.RefreshJobs:name=jobRefresher");
            jobSchedulerName = new ObjectName("azkaban.app.jmx.jobScheduler:name=jobScheduler");
            mbeanServer.registerMBean(new RefreshJobs(this), jobRefresherName);
            logger.info("Bean " + jobRefresherName.getCanonicalName() + " registered.");
            mbeanServer.registerMBean(new JobScheduler(_schedulerManager, _jobManager), jobSchedulerName);
            logger.info("Bean " + jobSchedulerName.getCanonicalName() + " registered.");
        }
        catch(Exception e) {
            logger.error("Failed to configure MBeanServer", e);
        }
    }

    public void close() {
        try {
            mbeanServer.unregisterMBean(jobRefresherName);
            mbeanServer.unregisterMBean(jobSchedulerName);
        } catch (Exception e) {
            logger.error("Failed to cleanup MBeanServer", e);
        }
    }

    public String getLogDirectory() {
        return _logsDir.getAbsolutePath();
    }

    public String getTempDirectory() {
        return _tempDir.getAbsolutePath();
    }

    public List<File> getJobDirectories() {
        return _jobDirs;
    }

    public JobExecutorManager getJobExecutorManager() {
        return _jobExecutorManager;
    }
    
    public ScheduleManager getScheduleManager() {
        return _schedulerManager;
    }
    
    public VelocityEngine getVelocityEngine() {
        return _velocityEngine;
    }

    public JobManager getJobManager() {
        return _jobManager;
    }

    public String getHdfsUrl() {
        return this._hdfsUrl;
    }

    public boolean hasHdfsUrl() {
        return this._hdfsUrl != null;
    }

    public ClassLoader getClassLoader() {
        return _baseClassLoader;
    }

    public String getAppInstanceName() {
        return _instanceName;
    }
    
    public FlowManager getAllFlows()
    {
        return _allFlows;
    }

    private ClassLoader getBaseClassloader() throws MalformedURLException
    {
        final ClassLoader retVal;

        String hadoopHome = System.getenv("HADOOP_HOME");
	String hadoopConfDir = System.getenv("HADOOP_CONF_DIR");

        if(hadoopConfDir != null) {
	  logger.info("Using hadoop config found in " + hadoopConfDir);
	  retVal = new URLClassLoader(new URL[] { new File(hadoopConfDir).toURI().toURL() },
				      getClass().getClassLoader());
	} else if(hadoopHome != null) {
            logger.info("Using hadoop config found in " + hadoopHome);
            retVal = new URLClassLoader(new URL[] { new File(hadoopHome, "conf").toURI().toURL() },
                                        getClass().getClassLoader());
        } else {
            logger.info("HADOOP_HOME not set, using default hadoop config.");
            retVal = getClass().getClassLoader();
        }

        return retVal;
    }

    private NamedPermitManager getNamedPermitManager(final Props props) throws MalformedURLException
    {
        int workPermits = props.getInt("total.job.permits", Integer.MAX_VALUE);
        NamedPermitManager permitManager = new NamedPermitManager();
        permitManager.createNamedPermit("default", workPermits);

        return permitManager;
    }

    private File getBackupFile(final Props defaultProps, final File initialJobDir)
    {
        File retVal = new File(initialJobDir.getAbsoluteFile(), "jobs.schedule.backup");

        String backupFile = defaultProps.getString("schedule.backup.file", null);
        if(backupFile != null) {
            retVal = new File(backupFile);
        } else {
            logger.info("Schedule backup file param not set. Defaulting to " + retVal.getAbsolutePath());
        }

        return retVal;
    }

    private File getScheduleFile(final Props defaultProps, final File initialJobDir)
    {
        File retVal = new File(initialJobDir.getAbsoluteFile(), "jobs.schedule");

        String scheduleFile = defaultProps.getString("schedule.file", null);
        if(scheduleFile != null) {
            retVal = new File(scheduleFile);
        } else {
            logger.info("Schedule file param not set. Defaulting to " + retVal.getAbsolutePath());
        }

        return retVal;
    }

    private long getLastExecutionId(final File executionsStorageDir)
    {
        long lastId = 0;

        for (File file : executionsStorageDir.listFiles()) {
            final String filename = file.getName();
            if (filename.endsWith(".json")) {
                try {
                    lastId = Math.max(
                            lastId,
                            Long.parseLong(filename.substring(0, filename.length() - 5))
                    );
                }
                catch (NumberFormatException e) {
                }
            }
        }

        return lastId;
    }

    
    public String getRuntimeProperty(final String name) {
        return _jobExecutorManager.getRuntimeProperty(name);
    }

    public void setRuntimeProperty(final String key, final String value) {
    	_jobExecutorManager.setRuntimeProperty(key, value);
    }
    
    public MonitorInterface getMonitor() {
        return _monitor;
    }
    
    public MonitorInternalInterface getInternalMonitor() {
        return _monitor;
    }
    
    public void reloadJobsFromDisk() {
        getJobManager().updateFlowManager();
    }
}
