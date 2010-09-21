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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;
import voldemort.serialization.json.JsonTypeSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class JsonSequenceFileInputFormat extends FileInputFormat<Object, Object>
{
    protected static final Logger log = Logger.getLogger(JsonSequenceFileInputFormat.class.getName());

    private final SequenceFileInputFormat<BytesWritable, BytesWritable> baseInputFormat = new SequenceFileInputFormat<BytesWritable, BytesWritable>();

    @Override
    public RecordReader<Object, Object> createRecordReader(
            final InputSplit split,
            final TaskAttemptContext context
    ) throws IOException
    {
        Configuration conf = context.getConfiguration();

        String inputPathString = ((FileSplit) split).getPath().toUri().getPath();
        log.info("Input file path:" + inputPathString);
        Path inputPath = new Path(inputPathString);

        SequenceFile.Reader reader =
                new SequenceFile.Reader(inputPath.getFileSystem(conf), inputPath, conf);
        SequenceFile.Metadata meta = reader.getMetadata();

        try
        {
            final Text keySchema = meta.get(new Text("key.schema"));
            final Text valueSchema = meta.get(new Text("value.schema"));


            if (0 == keySchema.getLength() || 0 == valueSchema.getLength())
            {
                throw new Exception(String.format("Cannot have a 0 length schema. keySchema[%s], valueSchema[%s]", keySchema, valueSchema));
            }

            return new JsonObjectRecordReader(
                    new JsonTypeSerializer(keySchema.toString()),
                    new JsonTypeSerializer(valueSchema.toString()),
                    baseInputFormat.createRecordReader(split, context)
            );
        }
        catch (Exception e)
        {
            throw new IOException("Failed to Load Schema from file:" + inputPathString + "\n");
        }
    }



    @Override
    protected List<FileStatus> listStatus(JobContext job) throws IOException
    {
        String dirs = job.getConfiguration().get("mapred.input.dir", "");
        String[] list = StringUtils.split(dirs);

        List<FileStatus> status = new ArrayList<FileStatus>();
        for (int i = 0; i < list.length; i++)
        {
            status.addAll(getAllSubFileStatus(job, new Path(list[i])));
        }

        return status;
    }

    private List<FileStatus> getAllSubFileStatus(JobContext jobContext, Path filterMemberPath) throws IOException
    {
        List<FileStatus> list = new ArrayList<FileStatus>();

        FileSystem fs = filterMemberPath.getFileSystem(jobContext.getConfiguration());
        FileStatus[] subFiles = fs.listStatus(filterMemberPath);

        if (null != subFiles)
        {
            if (fs.getFileStatus(filterMemberPath).isDir())
            {
                for (FileStatus subFile : subFiles)
                {
                    if (! subFile.getPath().getName().startsWith("_") )
                    {
                        list.addAll(getAllSubFileStatus(jobContext, subFile.getPath()));
                    }
                }
            }
            else
            {
                if ( subFiles.length > 0
                     && !subFiles[0].getPath().getName().startsWith("_") )
                {
                    list.add(subFiles[0]);
                }
            }
        }

        return list;
    }

    private static class JsonObjectRecordReader extends RecordReader<Object, Object>
    {
        final JsonTypeSerializer inputKeySerializer;
        final JsonTypeSerializer inputValueSerializer;
        final RecordReader<BytesWritable, BytesWritable> delegateReader;

        public JsonObjectRecordReader(
                final JsonTypeSerializer inputKeySerializer,
                final JsonTypeSerializer inputValueSerializer,
                final RecordReader<BytesWritable, BytesWritable> recordReader
        )
        {
            this.delegateReader = recordReader;
            this.inputKeySerializer = inputKeySerializer;
            this.inputValueSerializer = inputValueSerializer;
        }

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException
        {
            delegateReader.initialize(inputSplit, taskAttemptContext);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException
        {
            return delegateReader.nextKeyValue();
        }

        @Override
        public Object getCurrentKey() throws IOException, InterruptedException
        {
            return inputKeySerializer.toObject(delegateReader.getCurrentKey().getBytes());
        }

        @Override
        public Object getCurrentValue() throws IOException, InterruptedException
        {
            return inputValueSerializer.toObject(delegateReader.getCurrentValue().getBytes());
        }

        @Override
        public float getProgress() throws IOException, InterruptedException
        {
            return delegateReader.getProgress();
        }

        @Override
        public void close() throws IOException
        {
            delegateReader.close();
        }
    }
}
