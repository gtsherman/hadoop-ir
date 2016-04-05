package edu.gslis.hadoopir.datatypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import edu.gslis.searchhits.SearchHit;

public class SearchHitWritable implements Writable {

	private Text docno;
	private DoubleWritable score;
	
	public SearchHitWritable() {
		this("", 0.0);
	}
	
	public SearchHitWritable(SearchHit hit) {
		this(hit.getDocno(), hit.getScore());
	}

	public SearchHitWritable(String docno, double score) {
		this.docno = new Text(docno);
		this.score = new DoubleWritable(score);
	}
	
	public void readFields(DataInput in) throws IOException {
		docno.readFields(in);
		score.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		docno.write(out);
		score.write(out);
	}
	
	public SearchHit getSearchHit() {
		SearchHit hit = new SearchHit();
		hit.setDocno(docno.toString());
		hit.setScore(score.get());
		return hit;
	}

}
