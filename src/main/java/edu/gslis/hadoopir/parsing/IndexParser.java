package edu.gslis.hadoopir.parsing;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

import edu.gslis.searchhits.SearchHit;
import edu.gslis.searchhits.SearchHits;
import edu.gslis.textrepresentation.FeatureVector;
import edu.gslis.utils.SimpleLogger;

public class IndexParser {
	
	private static SimpleLogger logger = new SimpleLogger(IndexParser.class);
	
	public SearchHit parse(String indexFileLine) {
		SearchHit doc = new SearchHit();
		FeatureVector docVector = new FeatureVector(null);

		String[] parts = indexFileLine.split("\\t");

		String docId = parts[0];
		doc.setDocno(docId);
		double docLength = Double.parseDouble(parts[1]);
		doc.setLength(docLength);

		for (int i = 2; i < parts.length; i++) {
			String[] termFreq = parts[i].split(",");

			try {
				String term = termFreq[0];
				if (termFreq.length > 2) {
					for (int j = 1; j < termFreq.length - 1; j++) {
						term += ","+termFreq[j];
					}
				}
				double freq = Double.parseDouble(termFreq[termFreq.length-1]);
				docVector.setTerm(term, freq);
			} catch (IndexOutOfBoundsException e) {
				logger.error("Error parsing part: "+parts[i]);
			}
		}

		doc.setFeatureVector(docVector);
		return doc;
	}
	
	public SearchHits parse(File file) {
		SearchHits docs = new SearchHits();
		try {
			Scanner scanner = new Scanner(file);
			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();
				SearchHit doc = parse(line);
				docs.add(doc);
			}
			scanner.close();
		} catch (FileNotFoundException e) {
			logger.error("File not found: "+file.getAbsolutePath()+". Docs not parsed.");
		} 
		return docs;
	}
	
}
