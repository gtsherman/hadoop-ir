package edu.gslis.hadoopir.run;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.gslis.hadoopir.indexing.IndexCondenserJSON;
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
		if (config.get("data-dir") != null) {
			dataDir = config.get("data-dir");
		}
		
		String indexFile = "index";
		if (config.get("index-file") != null) {
			indexFile = config.get("index-file");
		}

		logger.info("Constructing intermediate index");
		TrecTextIndexer indexer = new TrecTextIndexer(dataDir);
		indexer.index(indexFile);
		
		logger.info("Condensing index");
		IndexCondenserJSON.run(config);
	}

}
