package azkaban.test;
    
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import azkaban.common.jobs.AbstractJob;
import azkaban.common.utils.Props;

@SuppressWarnings("deprecation")
public class WordCountGrid extends AbstractJob {
    
    private String _input = null;
    private String _output = null;
    
    public WordCountGrid(String id, Props prop)
    {
        super(id);
        _input = prop.getString("input");
        _output = prop.getString("output");
    }
    
       public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
         private final static IntWritable one = new IntWritable(1);
         private Text word = new Text();
    
         public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) 
         throws IOException {
           String line = value.toString();
           StringTokenizer tokenizer = new StringTokenizer(line);
           while (tokenizer.hasMoreTokens()) {
             word.set(tokenizer.nextToken());
         
             if (word.toString().equals("end_here")) { //expect an out-of-bound exception
                 String [] errArray = new String[1];
                 System.out.println("string in possition 2 is " + errArray[1]);
             }
             output.collect(word, one);
           }
         }
       }
    
       public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
         public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) 
         throws IOException {
           int sum = 0;
           while (values.hasNext()) {
             sum += values.next().get();
           }
           output.collect(key, new IntWritable(sum));
         }
       }

    
       public void run() throws Exception
       {
         JobConf conf = new JobConf(WordCountGrid.class);
         conf.setJobName("wordcount");
    
         conf.setOutputKeyClass(Text.class);
         conf.setOutputValueClass(IntWritable.class);
    
         conf.setMapperClass(Map.class);
         conf.setCombinerClass(Reduce.class);
         conf.setReducerClass(Reduce.class);
    
         Path outputPath = new Path(_output);
         outputPath.getFileSystem(conf).delete(outputPath);
         
         conf.setInputFormat(TextInputFormat.class);
         conf.setOutputFormat(TextOutputFormat.class);
    
         FileInputFormat.setInputPaths(conf, new Path(_input));
         FileOutputFormat.setOutputPath(conf, outputPath);
         JobClient.runJob(conf);
       }


      @Override
      public Props getJobGeneratedProperties()
      {
        return new Props();
      }
    }
    