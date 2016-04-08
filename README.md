# Hadoop IR

Code to perform information retrieval experiments in Hadoop. Very rough beginnings.

## Compiling

```
git clone https://github.com/gtsherman/hadoop-ir.git
mvn package
```

The jar file will be at `target/hadoopir-0.0.1-SNAPSHOT-jar-with-dependencies.jar`.

## Configuration files

Configuration files are required for all tasks. A simple `key: value` format is preferred; the `ir-utils` JSON parameter format is also supported.

Example:
```
data-dir: /path/to/corpus/directory/
index-file: /path/to/index/file
```

The current list of properties:

| Property | Values | Description | Default |
| -------- | ------ | ----------- | ------- |
| cluster-mode | (cosine \| kl)? | Specify clustering metric | "cosine" |
| cluster-output | {file path}? | Location to output clustering results | "clusters" |
| data-dir | {file path}? | Directory containing raw documents | "data" |
| index | {file path}? | Location of index; may be created, altered, or read | "index" |
| queries | {file path} | Location of queries file | |
| reducer-count | {number}? | Number of reducers to use; this property is not always respected | system default |
| results | {file path}? | Location to output search results | "results" |
| stoplist | {file path} | Location of stoplist |

## Indexing data

Data must be "indexed" before searching. This is not truly an index, but is a more condensed format to represent documents.

Run:
```
java -cp "/path/to/hadoopir-0.0.1-SNAPSHOT-jar-with-dependencies.jar" edu.gslis.hadoopir.run.Index /path/to/config/file
```

This will produce an index file and a `*.meta` file containing corpus-level statistics. Both need to placed into HDFS before searching:
```
hadoop fs -put index
hadoop fs -put index.meta
```

## Searching

In your configuration file, specify the index location (in HDFS), the desired result location (will be in HDFS), and the queries location:
```
index-file: index
results: searchResults
queries: /local/path/to/queries
```

Queries must be readable by `GQueryJsonImpl`. 

Then run the search:
```
hadoop jar /path/to/hadoopir-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.gslis.hadoopir.Search /path/to/config/file
```
