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
import azkaban.common.utils.Utils;
import azkaban.flow.ExecutableFlow;
import azkaban.flow.manager.FlowManager;
import azkaban.flow.JobManagerFlowDeserializer;
import azkaban.flow.manager.RefreshableFlowManager;
import azkaban.jobcontrol.impl.jobs.locks.NamedPermitManager;
import azkaban.jobcontrol.impl.jobs.locks.ReadWriteLockManager;
import azkaban.jobs.AzkabanCommandLine;
import azkaban.jobs.JavaJob;
import azkaban.jobs.JavaProcessJob;
import azkaban.jobs.PigProcessJob;
import azkaban.jobs.ProcessJob;
import azkaban.serialization.DefaultExecutableFlowSerializer;
import azkaban.serialization.ExecutableFlowSerializer;
import azkaban.serialization.de.ExecutableFlowDeserializer;
import azkaban.serialization.de.JobFlowDeserializer;
import azkaban.web.AzkabanServletContextListener;
import azkaban.web.JobManagerServlet;
import azkaban.web.LogServlet;
import azkaban.web.pages.ExecutionHistoryServlet;
import azkaban.web.pages.HdfsBrowserServlet;
import azkaban.web.pages.IndexServlet;
import azkaban.web.pages.JobDetailServlet;
import azkaban.web.pages.JobUploadServlet;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.log4j.Logger;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.log.Log4JLogChute;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;
import org.mortbay.http.HttpContext;
import org.mortbay.http.HttpServer;
import org.mortbay.http.SocketListener;
import org.mortbay.jetty.servlet.Default;
import org.mortbay.jetty.servlet.ServletHandler;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Master application that runs everything
 *
 * This class will be loaded up either by running from the command line or via a
 * servlet context listener.
 *
 * @author jkreps
 *
 */
public class AzkabanApp {

    private static final Logger logger = Logger.getLogger(AzkabanApp.class);
    private static final String DEFAULT_STATIC_DIR = "azkaban/web/static";
    private static final String DEFAULT_PLUGIN_DIR = "azkaban/plugin";

    private final List<File> _jobDirs;
    private final File _logsDir;
    private final File _tempDir;
    private final Scheduler _scheduler;
    private final VelocityEngine _velocityEngine;
    private final JobManager _jobManager;
    private final Mailman _mailer;
    private final ClassLoader _baseClassLoader;
    private final String _hdfsUrl;
    private final FlowManager _allFlows;

    public AzkabanApp(List<File> jobDirs, File logDir, File tempDir, boolean enableDevMode) throws IOException {
        this._jobDirs = Utils.nonNull(jobDirs);
        this._logsDir = Utils.nonNull(logDir);
        this._tempDir = Utils.nonNull(tempDir);

        if(!this._logsDir.exists())
            this._logsDir.mkdirs();

        if(!this._tempDir.exists())
            this._tempDir.mkdirs();

        for(File jobDir: _jobDirs) {
            if(!jobDir.exists()) {
                logger.warn("Job directory " + jobDir + " does not exist. Creating.");
                jobDir.mkdirs();
            }
        }

        if(jobDirs.size() < 1)
            throw new IllegalArgumentException("No job directory given.");

        Props defaultProps = PropsUtils.loadPropsInDirs(_jobDirs, ".properties", ".schema");

        _baseClassLoader = getBaseClassloader();

        NamedPermitManager permitManager = getNamedPermitManager(defaultProps);
        JobWrappingFactory factory = new JobWrappingFactory(
                permitManager,
                new ReadWriteLockManager(),
                _logsDir.getAbsolutePath(),
                "java",
                ImmutableMap.<String, Class<? extends Job>>of("java", JavaJob.class,
                                                              "command", ProcessJob.class,
                                                              "javaprocess", JavaProcessJob.class,
                                                              "pig", PigProcessJob.class)
        );

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
        int schedulerThreads = defaultProps.getInt("scheduler.threads", 50);

        final File initialJobDir = _jobDirs.get(0);
        File schedule = getScheduleFile(defaultProps, initialJobDir);
        File backup = getBackupFile(defaultProps, initialJobDir);
        File executionsStorageDir = new File(
                defaultProps.getString("azkaban.executions.storage.dir", initialJobDir.getAbsolutePath() + "/executions")
        );
        if (! executionsStorageDir.exists()) executionsStorageDir.mkdirs();
        long lastExecutionId = getLastExecutionId(executionsStorageDir);
        logger.info(String.format("Using path[%s] for storing executions.", executionsStorageDir));
        logger.info(String.format("Last known execution id was [%s]", lastExecutionId));

        final ExecutableFlowSerializer flowSerializer = new DefaultExecutableFlowSerializer();
        final ExecutableFlowDeserializer flowDeserializer = new ExecutableFlowDeserializer(
                new JobFlowDeserializer(
                        ImmutableMap.<String, Function<Map<String, Object>, ExecutableFlow>>of(
                                "jobManagerLoaded", new JobManagerFlowDeserializer(_jobManager, factory)
                        )
                )
        );
        _allFlows = new RefreshableFlowManager(_jobManager, factory, flowSerializer, flowDeserializer, executionsStorageDir, lastExecutionId);
        _jobManager.setFlowManager(_allFlows);

        this._scheduler = new Scheduler(_jobManager,
                                        _allFlows,
                                        _mailer,
                                        failureEmail,
                                        successEmail,
                                        _baseClassLoader,
                                        schedule,
                                        backup,
                                        schedulerThreads);

        this._velocityEngine = configureVelocityEngine(enableDevMode);
    }

    private VelocityEngine configureVelocityEngine(boolean devMode) {
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

    public String getLogDirectory() {
        return _logsDir.getAbsolutePath();
    }

    public String getTempDirectory() {
        return _tempDir.getAbsolutePath();
    }

    public List<File> getJobDirectories() {
        return _jobDirs;
    }

    public Scheduler getScheduler() {
        return _scheduler;
    }

    public VelocityEngine getVelocityEngine() {
        return _velocityEngine;
    }

    public JobManager getJobManager() {
        return _jobManager;
    }

    public static void main(String[] arguments) throws Exception {
        OptionParser parser = new OptionParser();
        OptionSpec<Integer> portOpt = parser.acceptsAll(Arrays.asList("p", "port"),
                                                        "The port on which to run the http server.")
                .withRequiredArg()
                .describedAs("port")
                .ofType(Integer.class);
        OptionSpec<Integer> httpThreadsOpt = parser.acceptsAll(Arrays.asList("http-threads"),
                                                               "The number of threads  http server.")
                .withRequiredArg()
                .describedAs("num_threads")
                .ofType(Integer.class);
        String devModeOpt = "dev-mode";
        parser.accepts(devModeOpt, "Enable developer friendly options.");
        OptionSpec<String> staticContentOpt = parser.accepts("static-dir",
                                                             "The static content directory for the web server.")
                .withRequiredArg()
                .describedAs("dir");

        AzkabanCommandLine cl = new AzkabanCommandLine(parser, arguments);
        OptionSet options = cl.getOptions();

        if(cl.hasHelp())
            cl.printHelpAndExit("USAGE: ./bin/azkaban-server.sh", System.out);

        if(cl.getJobDirs().size() == 0)
            cl.printHelpAndExit("No job directory given.", System.out);

        logger.info("Job log directory set to " + cl.getLogDir().getAbsolutePath());
        logger.info("Job directories set to " + cl.getJobDirs());

        AzkabanApp app = new AzkabanApp(cl.getJobDirs(), cl.getLogDir(), new File("temp"), options.has(devModeOpt));

        int portNumber = 8081;
        if(options.has(portOpt))
            portNumber = options.valueOf(portOpt);
        int httpThreads = 10;
        if(options.has(httpThreadsOpt))
            httpThreads = options.valueOf(httpThreadsOpt);
        final HttpServer server = new HttpServer();
        SocketListener listener = new SocketListener();
        listener.setPort(portNumber);
        listener.setMinThreads(1);
        listener.setMaxThreads(httpThreads);
        server.addListener(listener);

        HttpContext context = new HttpContext();
        context.setContextPath("/");
        context.setAttribute(AzkabanServletContextListener.AZKABAN_SERVLET_CONTEXT_KEY, app);
        server.addContext(context);

        String staticDir = options.has(staticContentOpt) ? options.valueOf(staticContentOpt)
                                                         : DEFAULT_STATIC_DIR;

        ServletHandler servlets = new ServletHandler();
        context.addHandler(servlets);
        context.setResourceBase(staticDir);
        servlets.addServlet("Static", "/static/*", Default.class.getName());
        servlets.addServlet("Index", "/", IndexServlet.class.getName());
        servlets.addServlet("Logs", "/logs", LogServlet.class.getName());
        servlets.addServlet("Job Detail", "/job", JobDetailServlet.class.getName());
        servlets.addServlet("Job Execution History",
                            "/history/*",
                            ExecutionHistoryServlet.class.getName());
        servlets.addServlet("Job Manager", "/api/jobs", JobManagerServlet.class.getName());
        servlets.addServlet("Job Upload", "/job-upload/*", JobUploadServlet.class.getName());
        servlets.addServlet("HDFS Browser", "/fs/*", HdfsBrowserServlet.class.getName());

        try {
            server.start();
        } catch(Exception e) {
            logger.warn(e);
            Utils.croak(e.getMessage(), 1);
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {

            public void run() {
                logger.info("Shutting down http server...");
                try {
                    server.stop();
                    server.destroy();
                } catch(Exception e) {
                    logger.error("Error while shutting down http server.", e);
                }
                logger.info("kk thx bye.");
            }
        });
        logger.info("Server running on port " + portNumber + ".");
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

    public FlowManager getAllFlows()
    {
        return _allFlows;
    }

    private ClassLoader getBaseClassloader() throws MalformedURLException
    {
        final ClassLoader retVal;

        String hadoopHome = System.getenv("HADOOP_HOME");
        if(hadoopHome == null) {
            logger.info("HADOOP_HOME not set, using default hadoop config.");
            retVal = getClass().getClassLoader();
        } else {
            logger.info("Using hadoop config found in " + hadoopHome);
            retVal = new URLClassLoader(new URL[] { new File(hadoopHome, "conf").toURL() },
                                        getClass().getClassLoader());
        }

        return retVal;
    }

    private NamedPermitManager getNamedPermitManager(Props props) throws MalformedURLException
    {
        int workPermits = props.getInt("total.job.permits", Integer.MAX_VALUE);
        NamedPermitManager permitManager = new NamedPermitManager();
        permitManager.createNamedPermit("default", workPermits);

        return permitManager;
    }

    private File getBackupFile(Props defaultProps, File initialJobDir)
    {
        File retVal = new File(initialJobDir.getAbsoluteFile(), "jobs.schedule.backup");

        String backupFile = defaultProps.getString("schedule.backup.file", null);
        if(backupFile != null)
            retVal = new File(backupFile);
        else
            logger.info("Schedule backup file param not set. Defaulting to " + retVal.getAbsolutePath());

        return retVal;
    }

    private File getScheduleFile(Props defaultProps, File initialJobDir)
    {
        File retVal = new File(initialJobDir.getAbsoluteFile(), "jobs.schedule");

        String scheduleFile = defaultProps.getString("schedule.file", null);
        if(scheduleFile != null)
            retVal = new File(scheduleFile);
        else
            logger.info("Schedule file param not set. Defaulting to " + retVal.getAbsolutePath());

        return retVal;
    }

    private long getLastExecutionId(File executionsStorageDir)
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
}
