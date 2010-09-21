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

package com.linkedin.json;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import voldemort.serialization.json.JsonTypeDefinition;
import voldemort.serialization.json.JsonTypeSerializer;

/**
 *
 */
public class JsonSequenceFileOutputFormat extends FileOutputFormat<Object, Object>
{
    @Override
    public RecordWriter<Object, Object> getRecordWriter(final TaskAttemptContext context) throws IOException, InterruptedException
    {
        // Shamelessly copy in hadoop code to allow us to set the metadata with our schema

        Configuration conf = context.getConfiguration();

        CompressionCodec codec = null;
        CompressionType compressionType = CompressionType.NONE;
        if (getCompressOutput(context)) {
            // find the kind of compression to do
            compressionType = SequenceFileOutputFormat.getOutputCompressionType(context);

            // find the right codec
            Class<?> codecClass = getOutputCompressorClass(context,
                                                           DefaultCodec.class);
            codec = (CompressionCodec)
                    ReflectionUtils.newInstance(codecClass, conf);
        }
        // get the path of the temporary output file
        Path file = getDefaultWorkFile(context, "");
        FileSystem fs = file.getFileSystem(conf);

        final String keySchema = getSchema("output.key.schema", conf);
        final String valueSchema = getSchema("output.value.schema", conf);

        /* begin cheddar's stealing of jay's code */
        SequenceFile.Metadata meta = new SequenceFile.Metadata();

        meta.set(new Text("key.schema"), new Text(keySchema));
        meta.set(new Text("value.schema"), new Text(valueSchema));

        final SequenceFile.Writer out =
                SequenceFile.createWriter(
                        fs,
                        conf,
                        file,
                        context.getOutputKeyClass(),
                        context.getOutputValueClass(),
                        compressionType,
                        codec,
                        context,
                        meta
                );
        /* end cheddar's stealing of jay's code */

        final JsonTypeSerializer keySerializer = new JsonTypeSerializer(keySchema);
        final JsonTypeSerializer valueSerializer = new JsonTypeSerializer(valueSchema);


        return new RecordWriter<Object, Object>() {

            public void write(Object key, Object value)
                    throws IOException {

                out.append(
                        new BytesWritable(keySerializer.toBytes(key)),
                        new BytesWritable(valueSerializer.toBytes(value))
                );
                context.progress();
            }

            public void close(TaskAttemptContext context) throws IOException {
                out.close();
            }
        };
    }

    private String getSchema(String prop, Configuration conf)
    {
        String schema = conf.get(prop);
        if (schema == null)
            throw new IllegalArgumentException("The required property '" + prop
                                               + "' is not defined in the JobConf for this Hadoop job.");
        // check that it is a valid schema definition
        JsonTypeDefinition.fromJson(schema);

        return schema;
    }

}
