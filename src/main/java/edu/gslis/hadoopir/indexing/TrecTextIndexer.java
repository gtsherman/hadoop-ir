package edu.gslis.hadoopir.indexing;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.io.FileUtils;

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
		
		File out = new File(indexFile);
		File metadata = new File(indexFile+".meta");
		
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
				
				try {
					FileUtils.writeStringToFile(out, doc+"\t"+fv.getLength(), true);
					
					Iterator<String> vecIt = fv.iterator();
					while (vecIt.hasNext()) {
						String term = vecIt.next();
						double termFreq = fv.getFeatureWeight(term);
						
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
						
						FileUtils.writeStringToFile(out, "\t"+term+","+termFreq, true);
					}
					FileUtils.writeStringToFile(out, "\n", true);
				} catch (IOException e) {
					logger.error("Error writing to index file.");
				}
			}
		}
		
		try {
			FileUtils.writeStringToFile(metadata, TOKEN_COUNT+"\t"+totalTokenCount+"\n", false);
			FileUtils.writeStringToFile(metadata, DOCUMENT_COUNT+"\t"+totalDocCount+"\n", true);
			for (String term : totalTermFreq.keySet()) {
				FileUtils.writeStringToFile(metadata, term+"\t"+totalTermFreq.get(term)+"\t"+totalTermDocFreq.get(term)+"\n", true);
			}
		} catch (IOException e) {
			logger.error("Error writing to index metadata file.");
		}
		
		logger.info("Indexed "+totalDocCount+" documents.");
	}

}
