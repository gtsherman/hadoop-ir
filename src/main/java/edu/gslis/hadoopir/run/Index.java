package edu.gslis.hadoopir.run;

import java.io.IOException;

import edu.gslis.hadoopir.indexing.IndexCondenserTabbed;
import edu.gslis.hadoopir.indexing.TrecTextIndexer;
import edu.gslis.utils.Configuration;
import edu.gslis.utils.SimpleConfiguration;
import edu.gslis.utils.SimpleLogger;

public class Index {

	private static SimpleLogger logger = new SimpleLogger(Index.class);
	
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		Configuration config = new SimpleConfiguration();
		config.read(args[0]);

		String dataDir = "data";
		if (config.get(SimpleConfiguration.DATA) != null) {
			dataDir = config.get(SimpleConfiguration.DATA);
		}
		
		String indexFile = "index";
		if (config.get(SimpleConfiguration.INDEX) != null) {
			indexFile = config.get(SimpleConfiguration.INDEX);
		}

		logger.info("Constructing intermediate index");
		TrecTextIndexer indexer = new TrecTextIndexer(dataDir);
		indexer.index(indexFile);
		
		logger.info("Condensing index");
		IndexCondenserTabbed.run(config);
	}

}
