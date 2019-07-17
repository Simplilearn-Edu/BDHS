# _Spark-Gradle_
A spark project with Java.

## Prerequisites
- [Java](https://java.com/en/download/)
- [Gradle](https://gradle.org/)

## Build and Demo process

### Clone the Repo
`git clone https://github.com/ragnar-lothbrok/spark-retail-transaction-analysis.git`

### Build
`./gradlew clean build`
### Run
`./gradlew run`
### All Together
`./gradlew clean run`

### Running Spark Jobs

/usr/local/spark-2.2.0-bin-hadoop2.6/bin/spark-submit --master yarn --deploy-mode cluster --class com.simplilearn.bigdata.spark.SparkLauncher spark-retail-transaction-analysis-1.0-SNAPSHOT-all.jar /tmp/all_transaction_1.csv yarn


/usr/local/spark-2.2.0-bin-hadoop2.6/bin/spark-submit --master yarn --deploy-mode cluster --class com.simplilearn.bigdata.spark.PersistSparkLauncher spark-retail-transaction-analysis-1.0-SNAPSHOT-all.jar /tmp/all_transaction_1.csv yarn http://namenode:60010/conf

/usr/local/spark-2.2.0-bin-hadoop2.6/bin/spark-submit --master yarn --deploy-mode cluster --class com.simplilearn.bigdata.spark.SparkStreamingRevenueReachedApp spark-retail-transaction-analysis-1.0-SNAPSHOT-all.jar <broker_url> transaction_data transaction_data yarn



/usr/local/spark-2.2.0-bin-hadoop2.6/bin/spark-submit --master yarn --deploy-mode cluster --class com.simplilearn.bigdata.spark.SparkStreamingTrendingCategoryMonthlyApp spark-retail-transaction-analysis-1.0-SNAPSHOT-all.jar <broker_url> transaction_data SparkStreamingTrendingCategoryMonthlyApp yarn 43200 5


/usr/local/spark-2.2.0-bin-hadoop2.6/bin/spark-submit --master yarn --deploy-mode cluster --class com.simplilearn.bigdata.spark.SparkStreamingTrendingBrandMonthlyByRevenueApp spark-retail-transaction-analysis-1.0-SNAPSHOT-all.jar <broker_url>  transaction_data SparkStreamingTrendingBrandMonthlyByRevenueApp yarn 43200 5


/usr/local/spark-2.2.0-bin-hadoop2.6/bin/spark-submit --master yarn --deploy-mode cluster --class com.simplilearn.bigdata.spark.SparkStreamingTrendingBrandMonthlyApp spark-retail-transaction-analysis-1.0-SNAPSHOT-all.jar <broker_url>  transaction_data SparkStreamingTrendingBrandMonthlyApp yarn 43200 5

### Checking Spark Job Logs
yarn logs -appOwner <Hadoop_user> -applicationId application_<job

###  Check job on yarn
http://<namenode_url>:8088/cluster
