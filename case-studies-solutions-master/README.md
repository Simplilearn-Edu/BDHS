# _Spark-Maven
A spark project with Java.

## Prerequisites
- [Java](https://java.com/en/download/)
- [Maven](https://maven.apache.org/)

## Build and Demo process

### Clone the Repo
`git clone https://github.com/ragnar-lothbrok/case-studies-solutions.git`

### Build
`mvn clean install`

### Running Spark Jobs

*/usr/local/spark-2.2.0-bin-hadoop2.6/bin/spark-submit --master yarn --deploy-mode cluster --class <class_name_with_package> <parameters>*

### Checking Spark Job Logs
*yarn logs -appOwner <Hadoop_user> -applicationId application_job_id*

###  Check job on yarn
http://<yarn_url>:8088/cluster


Create Directory and upload Jar file and all CSV Files

*hadoop dfs -mkdir -p /data/data_csvs/*

To change permission of directory

*hadoop dfs -chown <user>:supergroup  /data/data_csvs/*


*/usr/local/spark/spark-2.2.0-bin-hadoop2.6/bin/spark-submit --master yarn --deploy-mode cluster --class  com.simplilearn.bigdata.casestudy_10.Solution_1 case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar /data/data_csvs/all_companies_details_case_study_10.csv yarn*

*/usr/local/spark/spark-2.2.0-bin-hadoop2.6/bin/spark-submit --master yarn --deploy-mode cluster --class  com.simplilearn.bigdata.casestudy_10.Solution_2 case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar /data/data_csvs/all_companies_details_case_study_10.csv yarn*

*/usr/local/spark/spark-2.2.0-bin-hadoop2.6/bin/spark-submit --master yarn --deploy-mode cluster --class  com.simplilearn.bigdata.casestudy_10.Solution_3 case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar /data/data_csvs/all_companies_details_case_study_10.csv yarn*

*/usr/local/spark/spark-2.2.0-bin-hadoop2.6/bin/spark-submit --master yarn --deploy-mode cluster --class  com.simplilearn.bigdata.casestudy_10.Solution_4 case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar /data/data_csvs/all_companies_details_case_study_10.csv yarn*


*/usr/local/spark/spark-2.2.0-bin-hadoop2.6/bin/spark-submit --master yarn --deploy-mode cluster --class  com.simplilearn.bigdata.casestudy_13.Solution_1 case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar /data/data_csvs/flights_graph_case_study_13.csv yarn*

*/usr/local/spark/spark-2.2.0-bin-hadoop2.6/bin/spark-submit --master yarn --deploy-mode cluster --class  com.simplilearn.bigdata.casestudy_13.Solution_2 case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar /data/data_csvs/flights_graph_case_study_13.csv yarn*

*/usr/local/spark/spark-2.2.0-bin-hadoop2.6/bin/spark-submit --master yarn --deploy-mode cluster --class  com.simplilearn.bigdata.casestudy_13.Solution_3 case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar /data/data_csvs/flights_graph_case_study_13.csv yarn*


*/usr/local/spark/spark-2.2.0-bin-hadoop2.6/bin/spark-submit --master yarn --deploy-mode cluster --class  com.simplilearn.bigdata.casestudy_9.Solution_1 case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar /data/data_csvs/bus-breakdown-and-delays_case_study_9.csv yarn*

*/usr/local/spark/spark-2.2.0-bin-hadoop2.6/bin/spark-submit --master yarn --deploy-mode cluster --class  com.simplilearn.bigdata.casestudy_9.Solution_2 case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar /data/data_csvs/bus-breakdown-and-delays_case_study_9.csv yarn*

*/usr/local/spark/spark-2.2.0-bin-hadoop2.6/bin/spark-submit --master yarn --deploy-mode cluster --class  com.simplilearn.bigdata.casestudy_9.Solution_3 case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar /data/data_csvs/bus-breakdown-and-delays_case_study_9.csv yarn*

*/usr/local/spark/spark-2.2.0-bin-hadoop2.6/bin/spark-submit --master yarn --deploy-mode cluster --class  com.simplilearn.bigdata.casestudy_9.Solution_4 case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar /data/data_csvs/bus-breakdown-and-delays_case_study_9.csv yarn*


Steps to run in local

mvn clean install

*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar com.simplilearn.bigdata.casestudy_13.Solution_1 /Users/Downloads/CaseStudies/flights_graph_case_study_13.csv local*

*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar com.simplilearn.bigdata.casestudy_13.Solution_2 /Users/Downloads/CaseStudies/flights_graph_case_study_13.csv local*

*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar com.simplilearn.bigdata.casestudy_13.Solution_3 /Users/Downloads/CaseStudies/flights_graph_case_study_13.csv local*


*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar com.simplilearn.bigdata.casestudy_12.Solution_1 localhost:9092 transaction_data transaction_data local 5 5*

*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar com.simplilearn.bigdata.casestudy_12.Solution_2 localhost:9092 transaction_data transaction_data local 10 5*

*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar com.simplilearn.bigdata.casestudy_12.Solution_3 localhost:9092 transaction_data transaction_data local 10*

*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar com.simplilearn.bigdata.casestudy_10.Solution_1 /Users/Downloads/CaseStudies/all_companies_details_case_study_10.csv local*

*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar com.simplilearn.bigdata.casestudy_10.Solution_2 /Users/Downloads/CaseStudies/all_companies_details_case_study_10.csv local*

*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar com.simplilearn.bigdata.casestudy_10.Solution_3 /Users/Downloads/CaseStudies/all_companies_details_case_study_10.csv local*

*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar com.simplilearn.bigdata.casestudy_10.Solution_4 /Users/Downloads/CaseStudies/all_companies_details_case_study_10.csv local*


*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar  com.simplilearn.bigdata.casestudy_9.Solution_1 /Users/Downloads/CaseStudies/bus-breakdown-and-delays_case_study_9.csv local*

*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar  com.simplilearn.bigdata.casestudy_9.Solution_2 /Users/Downloads/CaseStudies/bus-breakdown-and-delays_case_study_9.csv local*

*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar  com.simplilearn.bigdata.casestudy_9.Solution_3 /Users/Downloads/CaseStudies/bus-breakdown-and-delays_case_study_9.csv local*

*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar  com.simplilearn.bigdata.casestudy_9.Solution_4 /Users/Downloads/CaseStudies/bus-breakdown-and-delays_case_study_9.csv local*


*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar  com.simplilearn.bigdata.casestudy_7.Solution_1 /Users/Downloads/CaseStudies/companies_case_study_7.csv*

*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar  com.simplilearn.bigdata.casestudy_7.Solution_2 /Users/Downloads/CaseStudies/companies_case_study_7.csv*

*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar  com.simplilearn.bigdata.casestudy_7.Solution_3 /Users/Downloads/CaseStudies/companies_case_study_7.csv*

*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar  com.simplilearn.bigdata.casestudy_6.DataUtil /Users/Downloads/CaseStudies/Traffic_accidents_by_time_of_occurrence_2001_2014_case_study_6.csv local <namenodes>*
  
*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar  com.simplilearn.bigdata.casestudy_6.Solution_1 <namenodes>*
  
*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar  com.simplilearn.bigdata.casestudy_6.Solution_2 <namenodes>*
  
*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar  com.simplilearn.bigdata.casestudy_6.Solution_3 <namenodes>*


*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar com.simplilearn.bigdata.casestudy_4.solution_1.Solution /Users/Downloads/CaseStudies/flights_Case_study_4.csv /Users/Documents/casestudiessolutions/solution_4_1*

*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar com.simplilearn.bigdata.casestudy_4.solution_2_1.Solution /Users/Downloads/CaseStudies/flights_Case_study_4.csv /Users/Documents/casestudiessolutions/solution_4_2 /Users/Downloads/CaseStudies/airlines.csv*

*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar com.simplilearn.bigdata.casestudy_4.solution_2_2.Solution /Users/Downloads/CaseStudies/flights_Case_study_4.csv /Users/Documents/casestudiessolutions/solution_4_3 /Users/Downloads/CaseStudies/airlines.csv*

*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar com.simplilearn.bigdata.casestudy_4.solution_3.Solution /Users/Downloads/CaseStudies/flights_Case_study_4.csv /Users/Documents/casestudiessolutions/solution_4_4*


*java -cp build/libs/kafka-producer-1.0-SNAPSHOT.jar com.simplilearn.bigdata.casestudy_3.solution_1.KafkaProducerTest localhost:9092 transaction_topic /Users/Downloads/CaseStudies/error.log_case_study_3.txt*
