package edu.gslis.hadoopir.run;

import edu.gslis.hadoopir.indexing.TrecTextIndexer;
import edu.gslis.utils.Configuration;
import edu.gslis.utils.SimpleConfiguration;

public class Index {

	public static void main(String[] args) {
		Configuration config = new SimpleConfiguration();
		config.read(args[0]);

		String dataDir = "data";
		if (config.get("data-dir") != null) {
			dataDir = config.get("data-dir");
		}
		
		String indexFile = "index";
		if (config.get("index-file") != null) {
			indexFile = config.get("index-file");
		}

		TrecTextIndexer indexer = new TrecTextIndexer(dataDir);
		indexer.index(indexFile);
	}

}
