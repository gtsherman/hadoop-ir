package edu.gslis.hadoopir.searching.output;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.gslis.hadoopir.datatypes.SearchHitWritable;
import edu.gslis.searchhits.SearchHit;

public class TrecOutputFormat<K, V> extends FileOutputFormat<K, V> {

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
		Path path = FileOutputFormat.getOutputPath(context);
		Path fullPath = new Path(path, "results");

		FileSystem fs = path.getFileSystem(context.getConfiguration());
		FSDataOutputStream fileOut = fs.create(fullPath, context);

		return new TrecRecordWriter<K, V>(fileOut);
	}
	
	public static class TrecRecordWriter<K, V> extends RecordWriter<K, V> {
		
		private DataOutputStream out;
		
		public TrecRecordWriter(DataOutputStream out) {
			this.out = out;
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			out.close();
		}

		@Override
		public void write(K key, V value) throws IOException, InterruptedException {
			int i = 1;
			for (SearchHitWritable hit : (SearchHitWritable[])((ArrayWritable) value).get()) {
				SearchHit doc = hit.getSearchHit();

				out.writeBytes(key.toString());
				out.writeBytes(" Q0");
				out.writeBytes(" "+doc.getDocno());
				out.writeBytes(" "+i++);
				out.writeBytes(" "+doc.getScore());
				out.writeBytes(" hadoopir\n");
			}
		}
		
	}

}
