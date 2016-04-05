package edu.gslis.docscoring.support;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import edu.gslis.hadoopir.indexing.TrecTextIndexer;
import edu.gslis.utils.SimpleLogger;

public class FileBackedCollectionStats extends CollectionStats {
	
	private static SimpleLogger logger = new SimpleLogger(FileBackedCollectionStats.class);

	private Map<String, Long> termFreq;
	private Map<String, Long> termDocFreq; 

	public FileBackedCollectionStats() {
		termFreq = new HashMap<String, Long>();
		termDocFreq = new HashMap<String, Long>();
	}

	@Override
	public double termCount(String term) {
		if (termFreq.containsKey(term))
			return (double) termFreq.get(term);
		logger.warn("Not in collection: "+term);
		return 1.0;
	}

	@Override
	public double docCount(String term) {
		if (termDocFreq.containsKey(term))
			return (double) termDocFreq.get(term);
		logger.warn("Not in collection: "+term);
		return 1.0;
	}

	public void setStatSource(File statSource) {
		try {
			Scanner scanner = new Scanner(statSource);
			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();
				logger.debug(line);
				String[] parts = line.split("\\t");
				if (parts.length == 2) { // Collection stats
					if (parts[0].equals(TrecTextIndexer.DOCUMENT_COUNT)) {
						docCount = (double) Long.parseLong(parts[1]);
					} else if (parts[0].equals(TrecTextIndexer.TOKEN_COUNT)) {
						tokCount = (double) Long.parseLong(parts[1]);
					}
				} else { // Vocabulary stats
					String term = parts[0];
					long thisTermFreq = Long.parseLong(parts[1]);
					termFreq.put(term, thisTermFreq);
					long thisTermDocFreq = Long.parseLong(parts[2]);
					termDocFreq.put(term, thisTermDocFreq);
				}
			}
			scanner.close();
			logger.debug("Doc count: "+docCount);
			logger.debug("Token count: "+tokCount);
		} catch (FileNotFoundException e) {
			logger.error("Error reading collection stats file.");
			e.printStackTrace();
		}
	}
	
	@Override
	public void setStatSource(String statSource) {
		// not needed
	}

}
