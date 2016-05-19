package com.hudvin.hadoop.playground.wiki;

/**
 * Created by kontiki on 10.05.16.
 */
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

public class ParseJsonDump {

    public static class WikiJsonMapper extends Mapper<Object, Text, Text, IntWritable> {

        private Gson gson = new Gson();

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (value.getLength()>1){
                String jsonString = value.toString().substring(0, value.getLength()-1);
                Map jsonStructure =  gson.fromJson(jsonString, Map.class);
                Map<String,Map> jsonLabels = (Map)jsonStructure.get("labels");
                Map<String, String> jsonEnLabel = jsonLabels.get("en");
                if(jsonEnLabel!=null){
                    System.out.println(jsonEnLabel);
                    word.set(jsonEnLabel.get("value"));
                    context.write(word, one);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "parse json dump");
        job.setJarByClass(ParseJsonDump.class);
        job.setMapperClass(WikiJsonMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //delete output dir if exists
        Path outputDir = new Path(args[1]);
        FileSystem fs = outputDir.getFileSystem(conf);
        if (outputDir.toString().contains("/tmp") && fs.exists(outputDir)){
            fs.delete(outputDir, true);
        }

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputDir);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
