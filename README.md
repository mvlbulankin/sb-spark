# sb-spark
## launch lab01
+ cd lab01/film_analysis
+ sbt package 
+ spark-submit --class film_analysis target/scala-2.11/film_analysis_2.11-1.0.jar

## launch lab02
+ cd lab02/website_relevance
+ sbt package 
+ spark-submit --class website_relevance target/scala-2.11/website_relevance_2.11-1.0.jar

## launch lab03
+ cd lab03/data_mart
+ sbt package 
+ spark-submit --packages org.elasticsearch:elasticsearch-spark-20_2.11:6.8.2,com.datastax.spark:spark-cassandra-connector_2.11:2.4.3,org.postgresql:postgresql:42.3.3 --class data_mart target/scala-2.11/data_mart_2.11-1.0.jar

## launch lab04a
+ cd lab04a/filter
+ sbt package
+ spark-submit --conf spark.filter.topic_name=lab04_input_data --conf spark.filter.offset=earliest --conf spark.filter.output_dir_prefix=/user/mihail.bulankin/visits --class filter --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 ./target/scala-2.11/filter_2.11-1.0.jar

## launch lab04b
+ cd lab04b/agg
+ sbt package
+ spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7 --class agg target/scala-2.11/agg_2.11-1.0.jar

## launch lab05
+ cd lab05/users_items
+ sbt package
+ spark-submit --conf spark.users_items.input_dir=/user/mihail.bulankin/visits --conf spark.users_items.output_dir=/user/mihail.bulankin/users-items --conf spark.users_items.update=0 --class users_items ./target/scala-2.11/users_items_2.11-1.0.jar

## launch lab06
+ cd lab06/features
+ sbt package
+ spark-submit --class features .target/scala-2.11/features_2.11-1.0.jar

## launch lab07
+ cd lab07/mlproject
+ sbt package
### training
+ spark-submit --class train target/scala-2.11/mlproject_2.11-1.0.jar
### inference
+ spark-submit --class test --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7 target/scala-2.11/mlproject_2.11-1.0.jar

### create kafka topic
+ /usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --topic mihail_bulankin_lab07_out --zookeeper spark-node-1.newprolab.com:2181 --partitions 1 --replication-factor 1
### clear kafka topic
+ /usr/hdp/current/kafka-broker/bin/kafka-topics.sh --zookeeper spark-node-1.newprolab.com:2181 --delete --topic mihail_bulankin_lab07_out

## download file from server
+ scp -i ~/.ssh/id_rsa_spark_de -r mihail.bulankin@spark-master-2.newprolab.com:/data/home/mihail.bulankin/ml-100k /Users/m.bulankin/spark_de_course/lab01

## upload file to server
+ scp -i ~/.ssh/id_rsa_spark_de -r lab01.json mihail.bulankin@spark-master-2.newprolab.com:/data/home/mihail.bulankin
