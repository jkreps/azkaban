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

import azkaban.common.utils.Utils;
import azkaban.jobs.AzkabanCommandLine;
import azkaban.web.ApiServlet;
import azkaban.web.AzkabanServletContextListener;
import azkaban.web.JobManagerServlet;
import azkaban.web.JobRunnerServlet;
import azkaban.web.LogServlet;
import azkaban.web.pages.ExecutionHistoryServlet;
import azkaban.web.pages.FlowExecutionServlet;
import azkaban.web.pages.HdfsBrowserServlet;
import azkaban.web.pages.IndexServlet;
import azkaban.web.pages.JobDetailServlet;
import azkaban.web.pages.JobUploadServlet;
import azkaban.web.pages.RefreshJobsServlet;

import java.io.File;
import java.util.Arrays;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.log4j.Logger;
import org.mortbay.http.HttpContext;
import org.mortbay.http.HttpServer;
import org.mortbay.http.SocketListener;
import org.mortbay.jetty.servlet.Default;
import org.mortbay.jetty.servlet.ServletHandler;


/**
 *
 */
public class AzkabanApp
{
  private static final Logger logger = Logger.getLogger(AzkabanApp.class);
  private static final String DEFAULT_STATIC_DIR = "azkaban/web/static";

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

      AzkabanApplication app = new AzkabanApplication(cl.getJobDirs(), cl.getLogDir(), new File("temp"), options.has(devModeOpt));

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
      servlets.addServlet("Job Runner", "/job-runner/*", JobRunnerServlet.class.getName());
      servlets.addServlet("HDFS Browser", "/fs/*", HdfsBrowserServlet.class.getName());
      servlets.addServlet("Api Servlet", "/call", ApiServlet.class.getName());
      servlets.addServlet("Flow Execution", "/flow", FlowExecutionServlet.class.getName());
      servlets.addServlet("favicon", "/favicon.ico", Default.class.getName());
      servlets.addServlet("Refresh Jobs", "/refresh-jobs", RefreshJobsServlet.class.getName());
      
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
}
