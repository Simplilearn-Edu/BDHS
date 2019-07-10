# _Spark-Maven
A spark project with Java.

## Prerequisites
- [Java](https://java.com/en/download/)
- [Maven](https://maven.apache.org/)

## Build and Demo process

### Clone the Repo
`git clone https://github.com/ragnar-lothbrok/scala-spark.git`

### Build
`mvn clean install`

### Running Spark Jobs

/usr/local/spark-2.2.0-bin-hadoop2.6/bin/spark-submit --master yarn --deploy-mode cluster --class com.simplilearn.bigdata.spark.SparkLauncher scala.spark-0.0.1-SNAPSHOT-jar-with-dependencies.jar /tmp/all_transaction_1.csv yarn


/usr/local/spark-2.2.0-bin-hadoop2.6/bin/spark-submit --master yarn --deploy-mode cluster --class com.simplilearn.bigdata.spark.PersistSparkLauncher scala.spark-0.0.1-SNAPSHOT-jar-with-dependencies.jar /tmp/all_transaction_1.csv yarn http://namenode:60010/conf

/usr/local/spark-2.2.0-bin-hadoop2.6/bin/spark-submit --master yarn --deploy-mode cluster --class com.simplilearn.bigdata.spark.SparkStreamingRevenueReachedApp scala.spark-0.0.1-SNAPSHOT-jar-with-dependencies.jar <broker_url> transaction_data transaction_data yarn



/usr/local/spark-2.2.0-bin-hadoop2.6/bin/spark-submit --master yarn --deploy-mode cluster --class com.simplilearn.bigdata.spark.SparkStreamingTrendingCategoryMonthlyApp scala.spark-0.0.1-SNAPSHOT-jar-with-dependencies.jar <broker_url> transaction_data SparkStreamingTrendingCategoryMonthlyApp yarn 43200 5


/usr/local/spark-2.2.0-bin-hadoop2.6/bin/spark-submit --master yarn --deploy-mode cluster --class com.simplilearn.bigdata.spark.SparkStreamingTrendingBrandMonthlyByRevenueApp scala.spark-0.0.1-SNAPSHOT-jar-with-dependencies.jar <broker_url>  transaction_data SparkStreamingTrendingBrandMonthlyByRevenueApp yarn 43200 5


/usr/local/spark-2.2.0-bin-hadoop2.6/bin/spark-submit --master yarn --deploy-mode cluster --class com.simplilearn.bigdata.spark.SparkStreamingTrendingBrandMonthlyApp scala.spark-0.0.1-SNAPSHOT-jar-with-dependencies.jar <broker_url>  transaction_data SparkStreamingTrendingBrandMonthlyApp yarn 43200 5

### Checking Spark Job Logs
yarn logs -appOwner <Hadoop_user> -applicationId application_<job

###  Check job on yarn
http://<namenode_url>:8088/cluster


Steps to run in local

1. mvn clean install
2. java -cp target/scala.spark-0.0.1-SNAPSHOT-jar-with-dependencies.jar  com.com.simplilearn.bigdata.spark.SparkLauncher /Users/raghugup/Downloads/dataset/complete_1/all_transaction_3.csv local
3. java -cp  target/scala.spark-0.0.1-SNAPSHOT-jar-with-dependencies.jar  com.com.simplilearn.bigdata.spark.SparkStreamingRevenueReachedApp localhost:9092 transaction_data transaction_data local
4. java -cp  target/scala.spark-0.0.1-SNAPSHOT-jar-with-dependencies.jar  com.com.simplilearn.bigdata.spark.SparkStreamingTrendingBrandMonthlyApp localhost:9092 transaction_data transaction_data local 2 5
5. java -cp  target/scala.spark-0.0.1-SNAPSHOT-jar-with-dependencies.jar  com.com.simplilearn.bigdata.spark.SparkStreamingTrendingBrandMonthlyByRevenueApp localhost:9092 transaction_data transaction_data local 2 5
6. java -cp  target/scala.spark-0.0.1-SNAPSHOT-jar-with-dependencies.jar  com.com.simplilearn.bigdata.spark.SparkStreamingTrendingCategoryMonthlyApp localhost:9092 transaction_data transaction_data local 2 5
7.java -cp  target/scala.spark-0.0.1-SNAPSHOT-jar-with-dependencies.jar  com.com.simplilearn.bigdata.spark.PersistSparkLauncher /Users/coder/Downloads/dataset/complete_1/all_transaction_3.csv local <namenodes>
