register ../../../target/hadoopir-0.0.1-SNAPSHOT-jar-with-dependencies.jar
docs = load '$data';
docs1 = foreach docs generate (*);
docs2 = foreach docs generate (*);
pairs = cross docs1, docs2;
sims = foreach pairs generate edu.gslis.hadoopir.pig.udf.KLSIM();
store sims into '$output';
