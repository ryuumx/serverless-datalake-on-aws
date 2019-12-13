# Building Serverless Data Lakes on AWS
Forked from Author: [Unni Pillai](github.com/unnipillai/techfest-building-serverless-datalake-on-aws) 

![Architecture Diagram](./img/unnik-techfest-2019-lab-architecture.png)

### Learning outcomes from this workshop?
* Design serverless data lake architecture
* Build a data processing pipeline and Data Lake using Amazon S3 for storing data
* Use Amazon Kinesis for real-time streaming data
* Use AWS Glue to automatically catalog datasets
* Run interactive ETL scripts in an Amazon SageMaker Jupyter notebook connected to an AWS Glue development endpoint
* Query data using Amazon Athena & visualize it using Amazon QuickSight

# Pre-requisites:  
* You need to have access to an AWS account with **AdminstratorAccess**
* This lab should be executed in **us-east-1** region
* Best is to **follow links from this  guide** & open them **in new a tab**
* Run this lab in a modern browser

# Syllabus

|Content| Link|
|:-----|:--:|
|Lab 1: Ingest and Storage |[Open Lab :arrow_forward:](./lab1)|
|Lab 2: Glue Data Catalog |[Open Lab :arrow_forward:](./lab2)|
|Lab 3: Serverless Spark ETL on Glue  |[Open Lab :arrow_forward:](./lab3)|
|Lab 3_nb: Serverless Spark ETL on Glue (using Sagemaker Notebook)  |[Open Lab :arrow_forward:](./lab3_nb)|
|Lab 4: Visualize Data with built-in ML transformations |[Open Lab :arrow_forward:](./lab4)|

# Clean Up

Failing to do this will result in incuring AWS usage charges.

Make sure you bring down / delete all resources created as part of this lab

## Resources to delete
* Kinesis Firehose Delivery Stream
	* GoTo: https://console.aws.amazon.com/firehose/home?region=us-east-1#/
	* Delete Firehose:  **aws-labseries-demo-stream**
* Glue Database
	* GoTo: https://console.aws.amazon.com/glue/home?region=us-east-1#catalog:tab=databases
	* Delete Database: **awslabs_db**
* Glue Crawler
	* GoTo: https://console.aws.amazon.com/glue/home?region=us-east-1#catalog:tab=crawlers
	* Delete Crawler: **CrawlDataFromKDG**
* Glue Dev Endpoint
	* GoTo: https://console.aws.amazon.com/glue/home?region=us-east-1#etl:tab=devEndpoints
	* Delete endpoint: **devendpoint1**
* Sagemaker Notebook
	* You may wish you download the notebook file locally on your laptop before deleting the notebook)
	* GoTo: https://console.aws.amazon.com/glue/home?region=us-east-1#etl:tab=notebooks
	* Delete Notebook: **aws-glue-notebook1**
* Delete IAM Role
	* GoTo: https://console.aws.amazon.com/iam/home?region=us-east-1#/roles
	* Search for AWSGlueServiceRoleLab
	* Delete Role: **AWSGlueServiceRoleLab**
* Delete S3 bucket
	* GoTo: https://s3.console.aws.amazon.com/s3/home?region=us-east-1
	* Delete Bucket: **YOUR_USERNAME-datalake-demo-bucket**
* Delete Cognito Setup :
	* Goto: https://us-west-2.console.aws.amazon.com/cloudformation/home?region=us-west-2#/stacks/
	* Click: **Kinesis-Data-Generator-Cognito-User**
	* Click: **Actions** > **DeleteStack**
	* On confirmation screen: Click: **Delete**
* Close QuickSight account
	* GoTo: https://us-east-1.quicksight.aws.amazon.com/sn/admin#permissions
	* Click: **Unsubscribe**
* Cognito Userpool
	* GoTo: https://us-west-2.console.aws.amazon.com/cognito/users/?region=us-west-2#/

* [extra] VPC Endpoint: 
	* GoTo: https://console.aws.amazon.com/vpc/home?region=us-east-1#Endpoints:sort=vpcEndpointId

* [extra] RDS clusters: 
	* GoTo: https://console.aws.amazon.com/rds/home?region=us-east-1#databases:
	
* [extra] Elastic IPs: 
	* GoTo: https://us-east-2.console.aws.amazon.com/ec2/v2/home?region=us-east-2#SecurityGroups:sort=groupId
	
