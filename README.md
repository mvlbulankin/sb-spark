# sb-spark
## launch lab03
+ cd lab03/data_mart
+ sbt package 
+ spark-submit --packages org.elasticsearch:elasticsearch-spark-20_2.11:6.8.2,com.datastax.spark:spark-cassandra-connector_2.11:2.4.3,org.postgresql:postgresql:42.3.3 --class data_mart target/scala-2.11/data_mart_2.11-1.0.jar

### example download file from server
scp -i ~/.ssh/id_rsa_spark_de -r mihail.bulankin@spark-master-2.newprolab.com:/data/home/mihail.bulankin/ml-100k /Users/m.bulankin/spark_de_course/lab01

### example upload file to server
scp -i ~/.ssh/id_rsa_spark_de -r lab01.json mihail.bulankin@spark-master-2.newprolab.com:/data/home/mihail.bulankin
