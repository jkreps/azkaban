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

package azkaban.web;

import azkaban.app.AzkabanApplication;
import java.io.File;
import java.io.IOException;
import java.util.Collections;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;


/**
* A ServletContextListener that loads the batch application
*
* @author jkreps
*
*/
public class AzkabanServletContextListener implements ServletContextListener {

    public static final String AZKABAN_SERVLET_CONTEXT_KEY = "azkaban_app";
    private static final String AZKABAN_HOME_VAR_NAME = "AZKABAN_HOME";

    private AzkabanApplication app;

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
        String homeDir = System.getenv(AZKABAN_HOME_VAR_NAME);
        if(homeDir == null)
            throw new IllegalStateException("The environment variable " + AZKABAN_HOME_VAR_NAME
                                            + " has not been set.");
        if(!new File(homeDir).isDirectory() || !new File(homeDir).canRead())
            throw new IllegalStateException(homeDir + " is not a readable directory.");
        try {
            File logDir = new File(homeDir, "logs");
            File jobDir = new File(homeDir, "jobs");
            File tempDir = new File(homeDir, "temp");

            this.app = new AzkabanApplication(Collections.singletonList(jobDir), logDir, tempDir, false);
        } catch(IOException e) {
            throw new IllegalArgumentException(e);
        }

        event.getServletContext().setAttribute(AZKABAN_SERVLET_CONTEXT_KEY, this.app);
    }

}
