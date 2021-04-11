# Serve query result with Lambda

In this module we are going to create a Lambda Function with a very specific use case example. The lambda function we are going to write will host the code for Athena to query and fetch Top 5 Popular Songs by Hits from processed data in S3.

![Architecture](.../img/unnik-techfest-2019-lab-architecture.png)

## 1. Creating a Lambda Function
1. Go to: https://console.aws.amazon.com/lambda/home?region=us-east-1

2. Click **Create function** (if you are using Lambda for the first time, then you might have to click Get Started to proceed)

3. Select **Author from scratch**

4. Under **Basic Information**:
        * Give Function name as **aws-labseries-demo-top5Songs**
        * Select Runtime as **Python 3.8**
        * Expand **Choose or create an execution role**, make sure **Create a new role with basic Lambda permissions** is selected.
5. Click **Create Function**

## 2. Author Lambda Function
In this section, we will provide code to the lambda function which we just created. We will use [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html?id=docs_gateway) to access Athena client.

1. Scroll down to Code Source section and replace existing code under in **lambda_function.py** with the python code below: 

    ```      
      import boto3
      import time
      import os
        
      # Environment Variables
      DATABASE = os.environ['DATABASE']
      TABLE = os.environ['TABLE']
      # Top X Constant
      TOPX = 5
      # S3 Constant
      S3_OUTPUT = f's3://{os.environ["BUCKET_NAME"]}/query_results/'
      # Number of Retries
      RETRY_COUNT = 10
        
      def lambda_handler(event, context):
          client = boto3.client('athena')
          # query variable with two environment variables and a constant
          query = f"""
              SELECT track_name as \"Track Name\", 
                      artist_name as \"Artist Name\",
                      count(1) as \"Hits\" 
              FROM {DATABASE}.{TABLE} 
              GROUP BY 1,2 
              ORDER BY 3 DESC
              LIMIT {TOPX};
          """
          response = client.start_query_execution(
              QueryString=query,
              QueryExecutionContext={ 'Database': DATABASE },
              ResultConfiguration={'OutputLocation': S3_OUTPUT}
          )
          query_execution_id = response['QueryExecutionId']
          # Get Execution Status
          for i in range(0, RETRY_COUNT):
              # Get Query Execution
              query_status = client.get_query_execution(
                  QueryExecutionId=query_execution_id
              )
              exec_status = query_status['QueryExecution']['Status']['State']
              if exec_status == 'SUCCEEDED':
                  print(f'Status: {exec_status}')
                  break
              elif exec_status == 'FAILED':
                  raise Exception(f'STATUS: {exec_status}')
              else:
                  print(f'STATUS: {exec_status}')
                  time.sleep(i)
          else:
              client.stop_query_execution(QueryExecutionId=query_execution_id)
              raise Exception('TIME OVER')
          # Get Query Results
          result = client.get_query_results(QueryExecutionId=query_execution_id)
          print(result['ResultSet']['Rows'])
          # Function can return results to your application or service
          # return result['ResultSet']['Rows']

      
    ```
    
2. Click to **Configuration** section:
    * In **General configuration** tab, change **Timeout** to **10** seconds.
    * Select **Environment variables** tab, add following variables:
            * Key: **DATABASE**, Value: **awslabs_db**
            * Key: **TABLE**, Value: **processed**
            * Key: **BUCKET_NAME**, Value: **YOUR_USERNAME-datalake-demo-bucket**
    * Select **Permisions** tab:
            * Click the **Role name** link under **Execution Role** to open the IAM Console in a new tab.
            * Click **Attach policies**
            * Add following 2 policies: **AmazonS3FullAccess** and **AmazonAthenaFullAccess**
            * Click **Attach policies**, then close this tab
        
## 3. Run the function
1. Deploy the function first by clicking on **Deploy** under the **Code** section. 

2. Click **Test** right next to it
    * A new window will pop up for us to configure test event. Input the Event name: **Test**. Then click **Create**
    * Click **Test** again
    * You should see the result in json format in **Execution result** tab

## More credits
1. Verify the result in Athena using the same query
2. Try trigger the Lambda function by a schedule using [CloudWatch Event](https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/Create-CloudWatch-Events-Scheduled-Rule.html)
3. Integrate this Lambda function to a HTTPS call using [API Gateway](https://docs.aws.amazon.com/lambda/latest/dg/services-apigateway.html)

