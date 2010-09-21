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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URLClassLoader;

/**
 *
 */
public class StagedOutputJob extends Job
{
    private final String stagingPrefix;
    private final Logger log;

    public StagedOutputJob(String stagingPrefix, Logger log) throws IOException
    {
        super();
        this.stagingPrefix = stagingPrefix;
        this.log = log;
    }

    public StagedOutputJob(Configuration conf, String stagingPrefix, Logger log) throws IOException
    {
        super(conf);
        this.stagingPrefix = stagingPrefix;
        this.log = log;
    }

    public StagedOutputJob(Configuration conf, String jobName, String stagingPrefix, final Logger log) throws IOException
    {
        super(conf, jobName);
        this.stagingPrefix = stagingPrefix;
        this.log = log;
    }

    @Override
    public boolean waitForCompletion(boolean verbose) throws IOException, InterruptedException, ClassNotFoundException
    {
        final Path actualOutputPath = FileOutputFormat.getOutputPath(this);
        final Path stagedPath = new Path(String.format("%s/%s/staged", stagingPrefix, System.currentTimeMillis()));

        FileOutputFormat.setOutputPath(
                this,
                stagedPath
        );

        final Thread hook = new Thread(new Runnable()
        {
            public void run()
            {
                try {
                    killJob();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });

        Runtime.getRuntime().addShutdownHook(hook);

        final boolean retVal = super.waitForCompletion(verbose);

        Runtime.getRuntime().removeShutdownHook(hook);

        if (retVal) {
            FileSystem fs = actualOutputPath.getFileSystem(getConfiguration());

            fs.mkdirs(actualOutputPath);

            log.info(String.format("Deleting data at old path[%s]", actualOutputPath));
            fs.delete(actualOutputPath, true);

            log.info(String.format("Moving from staged path[%s] to final resting place[%s]", stagedPath, actualOutputPath));
            return fs.rename(stagedPath, actualOutputPath);
        }

        log.warn("retVal was false for some reason...");
        return retVal;
    }
}
