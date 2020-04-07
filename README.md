# spark-nasa-topn

The Challenge: Write a program in Scala that downloads the dataset at 
ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz and uses 
Apache Spark to determine the top-n most frequent visitors and urls for each day of the trace.  
Package the application in a docker container.

PBI:
NASA001: (Completed in Databricks, TODO: in Docker)
As a Data Engineer I want be able to download the files from FTP so that I can process the file for parsing.

Code Repository: https://github.com/arnoldmdajao/spark-nasa-topn
Databricks Notebook: https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/1346925273388309/3939802469832194/4356266866585196/latest.html


Tasks
Create Function to copy data from ftp server
(For actual production) make sure that ftp/sftp connection is secure and encrypted
 


Test Cases
Function Fetch should be flexible to handle file name using regex patterns
Function results in exception when file is not found
Function returns exception file is found but zero bytes
Function should return the file with the same size as the source file
Function should be able to handle split large file 
(For Cloud) Function should copy the file to object storage like Amazon S3, GCP Cloud Storage or Amazon Blob 
(For on premise Hadoop) Function should copy the file to HDFS data lake
(For actual production)  Function returns exception if connection is not secured.

Acceptance Criteria
Files is transferred and file size is not zero bytes
(For actual production) ftp/sftp connection is secure and encrypted


PBI:
NASA001:
As a Data Engineer I want be able to parse files from FTP so that I can aggregate results of top-n
frequent visitors and urls for each day.

Tasks
Create Function reads data from sources to DataFrame : sourceToDataFrame
Create Function reads data DataFrame to Parsed DataFrame 
    using regex to parsed the logs row data: parseLogsDataFrame
Create Function that records unparsed data due to not conforming with the pattern which can 
    be saved in the table or output logs : getDiscardedDataFrame
Create a Function that can aggregate the data based on combination of columns '"log_date", "ip_address", "log_uri"'
    the TopN 'N' should be parameterized and also rank and dense_rank option is available: getRankedDataFrame

PBI:
As a Data Engineer I want be able to package the spark application in docker container 
so that I can easily run and deploy container to kubernetes

Create a docker image that can be deployed using Docker.  Kubernetes is also an option
Reference:  https://github.com/big-data-europe/docker-spark
Docker image built from this application: https://drive.google.com/open?id=1okexE7A8R9P0hcAfazEQRFoP1-cvqzBI (too big for github) 



