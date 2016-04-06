package edu.gslis.hadoopir.pig.udf.support;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

import edu.gslis.searchhits.SearchHit;
import edu.gslis.similarity.SimilarityScorer;
import edu.gslis.textrepresentation.FeatureVector;

public class GenericSimilarityUDF {

	public static String getSimilarity(Tuple tuple, SimilarityScorer scorer) throws IOException {
		try {
			Tuple doc1 = (Tuple) tuple.get(0);
			Tuple doc2 = (Tuple) tuple.get(1);

			SearchHit doc1Hit = readVector(doc1);
			SearchHit doc2Hit = readVector(doc2);
			
			double sim = scorer.score(doc1Hit.getFeatureVector(), doc2Hit.getFeatureVector());

			DecimalFormat format = new DecimalFormat("0.0000");
			return doc1Hit.getDocno()+","+doc2Hit.getDocno()+","+format.format(sim);
		} catch (Exception e) {
			throw new IOException("Couldn't process tuple, "+e);
		}
	}
	
	public static SearchHit readVector(Tuple tuple) throws ExecException {
		FeatureVector vector = new FeatureVector(null);

		DataByteArray dba = (DataByteArray) tuple.get(0);
		String docno = dba.toString();
		dba = (DataByteArray) tuple.get(1);
		double docLength = Double.parseDouble(dba.toString());

		for (int i = 2; i < tuple.size(); i++) {
			dba = (DataByteArray) tuple.get(i);
			String item = dba.toString();

			String[] termFreq = item.split(",");
			String term = termFreq[0];
			if (termFreq.length > 2) {
				for (int j = 1; j < termFreq.length - 1; j++) {
					term += ","+termFreq[j];
				}
			}
			double freq = Double.parseDouble(termFreq[termFreq.length-1]);
			
			vector.setTerm(term, freq);
		}
		SearchHit doc = new SearchHit();
		doc.setDocno(docno);
		doc.setLength(docLength);
		doc.setFeatureVector(vector);
		return doc;
	}
}
