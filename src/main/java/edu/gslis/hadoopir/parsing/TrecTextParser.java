package edu.gslis.hadoopir.parsing;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import edu.gslis.utils.SimpleLogger;

public class TrecTextParser {

	private static SimpleLogger logger = new SimpleLogger(TrecTextParser.class);
	
	private Map<String, String> docText;
	
	private StringBuilder docno;
	private StringBuilder text;
	
	private boolean inDocno = false;
	private boolean inText = false;
	
	public void parse(File file) {
		logger.info("Parsing "+file.getAbsolutePath());
		docText = new HashMap<String, String>();
		
		try {
			Scanner scanner = new Scanner(file);
			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();
				parse(line);
			}
			scanner.close();
		} catch (FileNotFoundException e) {
			logger.error("Couldn't find file.");
		}
	}
	
	public void parse(String line) {
		if (line.length() == 0) {
			return;
		}
		
		int cur = 0;
		int startBracket, endBracket;
		do {
			startBracket = line.indexOf('<', cur);
			startBracket = startBracket == -1 ? line.length()-1 : startBracket;
			endBracket = line.indexOf('>', cur);
			endBracket = endBracket == -1 ? line.length()-1 : endBracket;

			characters(line.substring(cur, startBracket));
			handleElement(line.substring(startBracket, endBracket+1));
			
			cur = endBracket+1;
		} while (cur < line.length());
		
	}

	public void startElement(String elementName) {
		if (elementName.equalsIgnoreCase("docno")) {
			inDocno = true;
			docno = new StringBuilder();
		} else if (elementName.equalsIgnoreCase("text")) {
			inText = true;
			text = new StringBuilder();
		}
	}
	
	public void endElement(String elementName) {
		if (elementName.equalsIgnoreCase("docno")) {
			inDocno = false;
		} else if (elementName.equalsIgnoreCase("text")) {
			inText = false;
		} else if (elementName.equalsIgnoreCase("doc")) {
			docText.put(docno.toString().trim(), text.toString().trim());
		}
	}
	
	public void handleElement(String element) {
		if (element.length() == 0) {
			return;
		}

		String elementName = element.replaceAll("[<>]", "");
		if (elementName.charAt(0) == '/') {
			endElement(elementName.substring(1));
		} else {
			startElement(elementName);
		}
	}
	
	public void characters(String token) {
		if (token.length() == 0) {
			return;
		}

		if (inDocno) {
			docno.append(token+" ");
		} else if (inText) {
			text.append(token+" ");
		}
	}
	
	public Map<String, String> getDocText() {
		return docText;
	}
}
