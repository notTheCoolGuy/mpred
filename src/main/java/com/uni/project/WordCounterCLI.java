package com.uni.project;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.BufferedReader;
import java.io.FileReader;

@SpringBootApplication
public class WordCounterCLI implements CommandLineRunner {

    @Autowired
    private WordMapper wordMapper;

    @Autowired
    private WordReducer wordReducer;

    @Override
    public void run(String... args) throws Exception {
        if (args.length != 1) {
            System.out.println("Usage: java -jar word-counter.jar <sentence>");
            return;
        }

        String sentence = args[0];
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word counter");

        TextInputFormat.addInputPath(job, new Path("input.txt"));
        TextOutputFormat.setOutputPath(job, new Path("output.txt"));

        job.setMapperClass(WordMapper.class);
        job.setReducerClass(WordReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.waitForCompletion(true);

        System.out.println("Word counts:");
        for (FileStatus fileStatus : FileSystem.get(conf).listStatus(new Path("output.txt"))) {
            try (BufferedReader reader = new BufferedReader(new FileReader(fileStatus.getPath().toString()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println(line);
                }
            }
        }
    }

    public static void main(String[] args) {
        SpringApplication.run(WordCounterCLI.class, args);
    }
}