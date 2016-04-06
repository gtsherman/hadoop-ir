package edu.gslis.hadoopir.pig.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import edu.gslis.hadoopir.pig.udf.support.GenericSimilarityUDF;
import edu.gslis.similarity.KLSimilarityScorer;

public class KLSIM extends EvalFunc<String> {

	@Override
	public String exec(Tuple tuple) throws IOException {
		if (tuple == null || tuple.size() == 0) {
			return null;
		}
		
		return GenericSimilarityUDF.getSimilarity(tuple, new KLSimilarityScorer());
	}

}
