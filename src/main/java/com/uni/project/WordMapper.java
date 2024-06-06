package com.uni.project;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class WordMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String sentence = value.toString();
        String[] words = sentence.split("\\s+");
        for (String word : words) {
            context.write(new Text(word.toLowerCase()), new LongWritable(1));
        }
    }
}