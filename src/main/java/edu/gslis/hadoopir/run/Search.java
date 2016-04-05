package edu.gslis.hadoopir.run;

import java.io.IOException;
import java.net.URISyntaxException;

import edu.gslis.hadoopir.searching.SearcherDirichlet;
import edu.gslis.queries.GQueriesJsonImpl;
import edu.gslis.utils.Configuration;
import edu.gslis.utils.SimpleConfiguration;

public class Search {

	public static void main(String[] args) throws IllegalArgumentException, ClassNotFoundException, IOException, InterruptedException, URISyntaxException {
		Configuration config = new SimpleConfiguration();
		config.read(args[0]);
		
		GQueriesJsonImpl queries = new GQueriesJsonImpl();
		queries.read(config.get("queries"));

		SearcherDirichlet searcher = new SearcherDirichlet();
		searcher.run(config, queries);
	}

}
