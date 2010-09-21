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

package com.linkedin.mr_kluj;

import azkaban.common.jobs.AbstractJob;
import clojure.lang.IFn;
import clojure.lang.RT;
import clojure.lang.Symbol;
import clojure.lang.Var;
import org.apache.hadoop.mapreduce.Job;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.URL;
import java.util.Properties;


/**
 *
 */
public class GenericClojureJob extends AbstractJob
{
    public static final String LI_CLJ_SOURCE = "li.clj.source";
    public static final String LI_CLJ_JOB_INDEX = "li.clj.job-index";
    public static final String LI_CLJ_PROPERTIES = "li.clj.properties";

    private final Properties props;

    public GenericClojureJob(String name, Properties props)
    {
        super(name);
        this.props = new Properties();

        for (String propKey : props.stringPropertyNames()) {
            this.props.setProperty(propKey, props.getProperty(propKey));
        }
    }

    public void run()
    {
        info("Starting " + getClass().getSimpleName());

        /*** Get clojure source ***/
        final String cljSource;
        if (props.getProperty(LI_CLJ_SOURCE) == null) {
            final String resourceName = props.getProperty("li.clj.source.file");
            if (resourceName == null) {
                throw new RuntimeException("Must define either li.clj.source or li.clj.source.file on the Props object.");
            }

            final URL resource = getClass().getClassLoader().getResource(resourceName);

            if (resource == null) {
                throw new RuntimeException(String.format("Resource[%s] does not exist on the classpath.", resourceName));
            }

            try {
                cljSource = new String(getBytes(resource.openStream()));
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }

            props.setProperty(LI_CLJ_SOURCE, cljSource);
        }
        else {
            cljSource = props.getProperty(LI_CLJ_SOURCE);
        }

        final String theActualFunction = String.format(
                "(require '[com.linkedin.mr-kluj.job :as job])\n\n" +
                "%s\n" +
                "(map (comp #(%%) job/starter) the-jobs)\n",
                cljSource
        );

        info("--- Source: ---");
        info(theActualFunction);
        info("       ---------       ");

        boolean jobCompleted;
        try {
            RT.var("clojure.core", "require").invoke(Symbol.intern("clojure.main"));

            Var.pushThreadBindings(
                    RT.map(
                            RT.var("clojure.core", "*warn-on-reflection*"), RT.T,
                            RT.var("user", "*context*"), null,
                            RT.var("user", "*props*"), props
                    )
            );

            Iterable<Job> jobs = (Iterable<Job>) clojure.lang.Compiler.load(new StringReader(theActualFunction), "start-job-input", "clj-job");

            int count = 0;
            for (Job job : jobs) {
                job.getConfiguration().set(LI_CLJ_SOURCE, cljSource);
                job.getConfiguration().set(LI_CLJ_JOB_INDEX, String.valueOf(count));

                ByteArrayOutputStream baos = new ByteArrayOutputStream(1024 * 10);
                props.storeToXML(baos, null);

                job.getConfiguration().set(LI_CLJ_PROPERTIES, new String(baos.toByteArray()));

                info(String.format("Starting job %s[%s]", job.getJobID(), job.getJobName()));

                jobCompleted = job.waitForCompletion(true);
                ++count;

                if (!jobCompleted) {
                    throw new RuntimeException(String.format("Job[%s] failed for some reason.", job.getJobID()));
                }
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public static void main(String[] args) throws IOException
    {
        final Properties props = new Properties();

        if (args.length % 2 != 1) {
            System.out.println("Usage: <java-command> clj-script-file key value key value");
            return;
        }

        props.put(LI_CLJ_SOURCE, new String(getBytes(new FileInputStream(args[0]))));
        for (int i = 1; i < args.length; i += 2) {
            props.put(args[i], args[i+1]);
        }

        new GenericClojureJob("scrap-job-Cheddar", props).run();
    }

    private static byte[] getBytes(final InputStream in) throws IOException
    {
        byte[] buffer = new byte[16*1024];

        ByteArrayOutputStream cljBytes = new ByteArrayOutputStream();

        int numRead;
        while((numRead = in.read(buffer)) > 0) {
            cljBytes.write(buffer, 0, numRead);
        }

        return cljBytes.toByteArray();
    }
}
