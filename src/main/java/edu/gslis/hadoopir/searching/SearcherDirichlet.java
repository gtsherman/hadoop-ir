package edu.gslis.hadoopir.searching;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.gslis.docscoring.ScorerDirichlet;
import edu.gslis.docscoring.support.FileBackedCollectionStats;
import edu.gslis.hadoopir.datatypes.SearchHitWritable;
import edu.gslis.hadoopir.parsing.IndexParser;
import edu.gslis.hadoopir.searching.output.TrecOutputFormat;
import edu.gslis.queries.GQueriesJsonImpl;
import edu.gslis.queries.GQuery;
import edu.gslis.searchhits.SearchHit;
import edu.gslis.searchhits.SearchHits;
import edu.gslis.utils.SimpleLogger;

public class SearcherDirichlet {
	
	public static class SearcherDirichletMap extends Mapper<LongWritable, Text, Text, SearchHitWritable> {

		private static SimpleLogger logger = new SimpleLogger(SearcherDirichletMap.class);

		GQueriesJsonImpl queries;
		FileBackedCollectionStats cs;
		
		public void setup(Context context) throws IOException {
			URI[] paths = context.getCacheFiles();
			
			String queriesPath = paths[0].getPath();
			logger.debug("Queries path: "+queriesPath);
			queries = new GQueriesJsonImpl();
			queries.read(queriesPath);
			logger.info(queries.numQueries()+" queries loaded.");
			
			// Get collection stats
			String metaPath = paths[1].getPath();
			logger.debug("Meta path: "+metaPath);
			cs = new FileBackedCollectionStats();
			File metaFile = new File(metaPath);
			cs.setStatSource(metaFile);
		}
		
		@Override
		public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
			IndexParser parser = new IndexParser();
			ScorerDirichlet scorer = new ScorerDirichlet();
			scorer.setCollectionStats(cs);
			String docLine = line.toString();

			SearchHit parsedDoc = parser.parse(docLine);
			Iterator<GQuery> queryIt = queries.iterator();
			while (queryIt.hasNext()) {
				GQuery query = queryIt.next();

				logger.debug("Working on query "+query.getTitle());

				SearchHit doc = new SearchHit();
				doc.setDocno(parsedDoc.getDocno());
				doc.setFeatureVector(parsedDoc.getFeatureVector());
				doc.setLength(parsedDoc.getLength());
				
				scorer.setQuery(query);
				double score = scorer.score(doc);

				logger.debug("Score for "+doc.getDocno()+": "+score);

				doc.setScore(score);

				context.write(new Text(query.getTitle()), new SearchHitWritable(doc));
			}
		}
	}

	public static class SearcherDirichletReduce extends Reducer<Text, SearchHitWritable, Text, ArrayWritable> {

		private static SimpleLogger logger = new SimpleLogger(SearcherDirichletReduce.class);
		
		@Override
		public void reduce(Text query, Iterable<SearchHitWritable> docIt, Context context) throws IOException, InterruptedException {
			logger.debug("Working on query: "+query);

			SearchHits docs = new SearchHits();
			for (SearchHitWritable doc : docIt) {
				docs.add(doc.getSearchHit());
			}
			docs.rank();
			docs.crop(1000);
			
			SearchHitWritable[] searchHits = new SearchHitWritable[docs.hits().size()];

			Iterator<SearchHit> docsIt = docs.iterator();
			int i = 0;
			while (docsIt.hasNext()) {
				SearchHit doc = docsIt.next();
				searchHits[i++] = new SearchHitWritable(doc);
			}

			ArrayWritable hits = new ArrayWritable(SearchHitWritable.class);
			hits.set(searchHits);
			
			context.write(query, hits);
		}

	}
	
	public void run(edu.gslis.utils.Configuration params) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "search");
		job.setJarByClass(SearcherDirichlet.class);
		
		job.setMapperClass(SearcherDirichletMap.class);
		job.setReducerClass(SearcherDirichletReduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(SearchHitWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ArrayWritable.class);
		
		// Set up input and output
		String inputPath = params.get("index-file");
		String outputPath = params.get("results");
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.setOutputFormatClass(TrecOutputFormat.class);
		
		// Metadata and queries in distributed cache
		job.addCacheFile(new URI(params.get("queries")));
		job.addCacheFile(new URI(inputPath+".meta"));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
