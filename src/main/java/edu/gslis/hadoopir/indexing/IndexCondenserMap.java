package edu.gslis.hadoopir.indexing;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IndexCondenserMap extends Mapper<LongWritable, Text, Text, Text> {
	
	public void map(LongWritable lineKey, Text line, Context context) throws IOException, InterruptedException {
		String[] lineParts = line.toString().split("\\t");

		String term = lineParts[0];
		String docFreq = lineParts[1]+"\t"+lineParts[2];

		context.write(new Text(term), new Text(docFreq));
	}

}
