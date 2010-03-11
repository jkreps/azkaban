package azkaban.web;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import azkaban.app.AzkabanApp;

/**
 * A ServletContextListener that loads the batch application
 * 
 * @author jkreps
 * 
 */
public class AzkabanServletContextListener implements ServletContextListener {

    public static final String BATCH_SERVLET_CONTEXT_KEY = "azkaban_app";
    private static final String BATCH_HOME_VAR_NAME = "AZKABAN_HOME";

    private AzkabanApp app;

    /**
     * Delete the app
     */
    public void contextDestroyed(ServletContextEvent event) {
        this.app = null;
    }

    /**
     * Load the app
     */
    public void contextInitialized(ServletContextEvent event) {
        String homeDir = System.getenv(BATCH_HOME_VAR_NAME);
        if(homeDir == null)
            throw new IllegalStateException("The environment variable " + BATCH_HOME_VAR_NAME
                                            + " has not been set.");
        if(!new File(homeDir).isDirectory() || !new File(homeDir).canRead())
            throw new IllegalStateException(homeDir + " is not a readable directory.");
        try {
            File logDir = new File(homeDir, "logs");
            File jobDir = new File(homeDir, "jobs");
            File tempDir = new File(homeDir, "temp");

            this.app = new AzkabanApp(Collections.singletonList(jobDir), logDir, tempDir, false);
        } catch(IOException e) {
            throw new IllegalArgumentException(e);
        }

        event.getServletContext().setAttribute(BATCH_SERVLET_CONTEXT_KEY, this.app);
    }

}
