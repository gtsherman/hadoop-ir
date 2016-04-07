package edu.gslis.hadoopir.indexing;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.gslis.hadoopir.parsing.TrecTextParser;
import edu.gslis.textrepresentation.FeatureVector;
import edu.gslis.utils.SimpleLogger;

public class TrecTextIndexer implements Indexer {

	private static SimpleLogger logger = new SimpleLogger(TrecTextIndexer.class);
	
	public static final String DOCUMENT_COUNT = "DOCS";
	public static final String TOKEN_COUNT = "TOKENS";
	
	private TrecTextParser parser;
	private File dir;
	
	public TrecTextIndexer() {
		this(null);
	}
	
	public TrecTextIndexer(String directory) {
		setDirectory(directory);
		parser = new TrecTextParser();
	}
	
	public void setDirectory(String directory) {
		File dir = new File(directory);
		if (!dir.isDirectory()) {
			logger.warn("You must specify a directory to index. Directory not set.");
			return;
		}
		this.dir = dir;
	}
	
	public void index(String indexFile) {
		if (dir == null) {
			logger.warn("Set the data directory before indexing. Data was not indexed.");
			return;
		}
		
		try {
			/*
			FileWriter out = new FileWriter(indexFile+".inter", true);
			FileWriter metadata = new FileWriter(indexFile+".meta", true);
			*/
			
			FileSystem fs = FileSystem.get(new Configuration());
			Path outPath = new Path(indexFile+".inter");
			Path metadataPath = new Path(indexFile+".meta");
			BufferedWriter out = new BufferedWriter(new OutputStreamWriter(fs.create(outPath, true)));
			BufferedWriter metadata = new BufferedWriter(new OutputStreamWriter(fs.create(metadataPath, true)));

			long totalTokenCount = 0;
			long totalDocCount = 0;
			Map<String, Long> totalTermFreq = new HashMap<String, Long>();
			Map<String, Long> totalTermDocFreq = new HashMap<String, Long>();

			for (File file : dir.listFiles()) {
				parser.parse(file);
			
				Map<String, String> docText = parser.getDocText();
				for (String doc : docText.keySet()) {
					String text = docText.get(doc);

					FeatureVector fv = new FeatureVector(null);
					fv.addText(text);
					
					totalDocCount++;
					totalTokenCount += fv.getLength();
					
					//FileUtils.writeStringToFile(out, doc+"\t"+fv.getLength(), true);
					
					Iterator<String> vecIt = fv.iterator();
					while (vecIt.hasNext()) {
						String term = vecIt.next();
						int termFreq = (int) fv.getFeatureWeight(term);
						
						// Update total term freq
						if (totalTermFreq.get(term) == null) {
							totalTermFreq.put(term, 0L);
						}
						totalTermFreq.put(term, totalTermFreq.get(term) + (long) termFreq);

						// Update total term doc freq
						if (totalTermDocFreq.get(term) == null) {
							totalTermDocFreq.put(term, 0L);
						}
						totalTermDocFreq.put(term, totalTermDocFreq.get(term) + 1);
						
						out.write(term+"\t"+doc+"\t"+termFreq+"\n");
					}
				}
			}
		
			metadata.write(TOKEN_COUNT+"\t"+totalTokenCount+"\n");
			metadata.write(DOCUMENT_COUNT+"\t"+totalDocCount+"\n");
			for (String term : totalTermFreq.keySet()) {
				metadata.write(term+"\t"+totalTermFreq.get(term)+"\t"+totalTermDocFreq.get(term)+"\n");
			}
			
			out.close();
			metadata.close();
		
			logger.info("Indexed "+totalDocCount+" documents.");
		} catch (IOException e) {
			logger.error("Error writing to index or metadata file: "+e);
		}
	}

}
