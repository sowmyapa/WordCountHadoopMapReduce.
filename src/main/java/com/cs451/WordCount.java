package com.cs451;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by sowmyaparameshwara on 11/21/16.
 * Hadoop WordCount Program
 * Mapper : Input is each line of the input file. It splits the line into words filters the special characters
 *          and writes the word with value 1 to the output
 * Reducer : Input key is the word and inut value if the list of the words count. It adds all this list of values
 *           and computes the final count for that word.
 * Main Driver Class : Configures Hadoop Job parameters and starts the job.
 *
 * Maven is used as a build tool here.
 * Use : mvn clean install to rebuild jar.
 *
 * How to run :
 * 1) Copy jar to hadoop machine : cp -i ~/.ssh/MyKeyPair.pem -r /Users/sowmyaparameshwara/Desktop/Assignments/PDC_Assignments/WordCountMapReduce/target/WordCountMapReduce-1.0-SNAPSHOT.jar ec2-user@35.162.162.230:/home/ec2-user
 * 2) Start Hadoop : sbin/start-dfs.sh
 * 3) Create input directory : bin/hadoop dfs -mkdir -p /user/sparameshwara/input
 * 4) Put the input file into hdfs : bin/hadoop dfs  -put dummy_input.txt /user/sparameshwara/input
 * 5) Run MapReduce application : bin/hadoop jar /home/ec2-user/WordCountMapReduce-1.0-SNAPSHOT.jar /user/sparameshwara/input /user/sparameshwara/Output
 * 6) Get output to local : bin/hadoop dfs -get /user/sparameshwara/Output .
 * 7) View output : cat Output/*
 */
public class WordCount {

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                String newstr = tokenizer.nextToken().replaceAll("[^A-Za-z]+", "");
                word.set(newstr);
                context.write(word, one);
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> intermediateValues, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable intermediateValue : intermediateValues) {
                sum += intermediateValue.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        Configuration conf = new Configuration();

        Job job = new Job(conf, "WordCount");

        job.setJarByClass(WordCount.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
        long endTime = System.currentTimeMillis();
        System.out.println(" Total Time in ms : "+(endTime-startTime));
    }
}
