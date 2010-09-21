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


import clojure.lang.IFn;
import clojure.lang.ISeq;
import clojure.lang.RT;
import clojure.lang.Symbol;
import clojure.lang.Var;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 *
 */
public class ClojureMapper extends Mapper<Object, Object, Object, Object>
{
    IFn theFunction;
    long currTime;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        Configuration config = context.getConfiguration();
        Properties props = new Properties();
        props.loadFromXML(new ByteArrayInputStream(config.get(GenericClojureJob.LI_CLJ_PROPERTIES).getBytes()));

        int jobIndex = context.getConfiguration().getInt(GenericClojureJob.LI_CLJ_JOB_INDEX, -1);
        if (jobIndex == -1) {
            throw new RuntimeException(
                    String.format("Expected property[%s] to be set on the Configuration object.  It wasn't.", GenericClojureJob.LI_CLJ_JOB_INDEX)
            );
        }

        final String theActualFunction = String.format(
                "(require '[com.linkedin.mr-kluj.job :as job])\n\n" +
                "%s\n" +
                "((comp job/map-function job/mapper) (nth the-jobs %s))",
                context.getConfiguration().get(GenericClojureJob.LI_CLJ_SOURCE),
                jobIndex
        );

        currTime = System.currentTimeMillis();
        try {
            RT.var("clojure.core", "require").invoke(Symbol.intern("clojure.main"));

            Var.pushThreadBindings(
                    RT.map(
                            RT.var("clojure.core", "*warn-on-reflection*"), RT.T,
                            RT.var("user", "*context*"), context,
                            RT.var("user", "*props*"), props
                    )
            );

            this.theFunction = (IFn) clojure.lang.Compiler.load(new StringReader(theActualFunction), "clj-mapper", "source-clj");
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        System.out.printf("Clojure mapper startup in %s millis.%n", System.currentTimeMillis() - currTime);
        currTime = System.currentTimeMillis();
    }


    @Override
    protected void map(Object key, Object value, Context context) throws IOException, InterruptedException
    {
        try {
            theFunction.invoke(key, value, context);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
        Var.popThreadBindings();
        System.out.printf("Mapper ran in %s millis.%n", System.currentTimeMillis() - currTime);
    }
}
