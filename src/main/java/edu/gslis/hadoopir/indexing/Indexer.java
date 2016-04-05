package edu.gslis.hadoopir.indexing;

public interface Indexer {

	/**
	 * For some inputs, produce an index file.
	 * @param indexFile	The location to write the index file.
	 */
	public void index(String indexFile);
	
}
