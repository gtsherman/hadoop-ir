package edu.gslis.hadoopir.indexing;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.simple.JSONObject;

public class IndexCondenserJSON {
	
	public static class IndexCondenserJSONReduce extends Reducer<Text, Text, Text, Text> {
		
		@SuppressWarnings("unchecked")
		public void reduce(Text term, Iterable<Text> docFreqs, Context context) throws IOException, InterruptedException {
			JSONObject json = new JSONObject();
			
			for (Text docFreq : docFreqs) {
				String[] docFreqParts = docFreq.toString().split("\\t");
				json.put(docFreqParts[0], docFreqParts[1]);
			}
			
			context.write(term, new Text(json.toJSONString()));
		}

	}

	public static void run(edu.gslis.utils.Configuration params) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "index");
		job.setJarByClass(IndexCondenserJSONReduce.class);
		
		job.setMapperClass(IndexCondenserMap.class);
		job.setReducerClass(IndexCondenserJSONReduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// Set up input and output
		String inputPath = params.get("index-file")+".inter";
		String outputPath = params.get("index-file");
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.setNumReduceTasks(Integer.parseInt(params.get("reducer-count")));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
