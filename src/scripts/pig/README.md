# Pig Scripts

These scripts for Apache Pig may be useful for certain IR-related tasks. Pig scripts should be invoked with:
```
pig -param input=inputFile -param output=outputDir script.pig
```
where `inputFile` and `outputDir` are located in HDFS. To use local files, simply add `-x local` before the first parameter argument (but you should probably use these scripts in distributed mode).

## Cosine similarity and KL similarity

These scripts take "indexed" documents as input and compute the similarity between each pair of documents. They are extremely slow to compute, at present. The cosine similarity script ensures each pair occurs only once since cosine similarity is symmetrical, while the KL similarity script computes each pair in each direction (which makes it even slower than the cosine computations!).
