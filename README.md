# sb-spark
## launch lab03
+ cd lab03/data_mart
+ sbt package 
+ spark-submit --packages org.elasticsearch:elasticsearch-spark-20_2.11:6.8.2,com.datastax.spark:spark-cassandra-connector_2.11:2.4.3,org.postgresql:postgresql:42.3.3 --class data_mart target/scala-2.11/data_mart_2.11-1.0.jar

## launch lab04a
+ cd lab04a/filter
+ sbt package
+ spark-submit --conf spark.filter.topic_name=lab04_input_data --conf spark.filter.offset=earliest --conf spark.filter.output_dir_prefix=/user/mihail.bulankin/visits --class filter --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 ./target/scala-2.11/filter_2.11-1.0.jar

## launch lab05
+ cd lab05/users_items
+ sbt package
+ spark-submit --conf spark.users_items.input_dir=/user/mihail.bulankin/visits --conf spark.users_items.output_dir=/user/mihail.bulankin/users-items --conf spark.users_items.update=0 --class users_items ./target/scala-2.11/users_items_2.11-1.0.jar

## download file from server
scp -i ~/.ssh/id_rsa_spark_de -r mihail.bulankin@spark-master-2.newprolab.com:/data/home/mihail.bulankin/ml-100k /Users/m.bulankin/spark_de_course/lab01

## upload file to server
scp -i ~/.ssh/id_rsa_spark_de -r lab01.json mihail.bulankin@spark-master-2.newprolab.com:/data/home/mihail.bulankin
