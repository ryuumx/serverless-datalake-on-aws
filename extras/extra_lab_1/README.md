# Monitoring data transfer costs using Athena and S3 Bucket Access Logging

![Architecture](./img/architecture.png)

## 1. Create a bucket `YOUR_USERNAME-s3-dto-logs`. 
* This bucket should only be accessible to audit-level users. You can set this as Bucket policy or add a Deny rule in your IAM policies.

## 2. Create a source s3 bucket `YOUR_USERNAME-s3-demo-bucket`
This is to store source data, or you can modify an existing bucket. 
* In step 2: Configure options, turn on Server access logging. 

![step 2](./img/1_configure_access_logging_2.png)

* Set the target bucket as the `YOUR_USERNAME-dto-logs` bucket you created in **Step 1**.

## 3. Perform some tasks to upload/remove data from the source bucket `YOUR_USERNAME-demo-bucket`.
For example, download / add / or delete objects from the bucket.

Inspect the `YOUR_USERNAME-dto-log` bucket. You should now see new logs in your target bucket. 

An access log structure looks like this: 

    ```      
      79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be awsexamplebucket [06/Feb/2019:00:00:38 +0000] 192.0.2.3 79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be 3E57427F3EXAMPLE REST.GET.VERSIONING - "GET /awsexamplebucket?versioning HTTP/1.1" 200 - 113 - 7 - "-" "S3Console/0.4" - 
      
    ```

The fields are listed [here](https://docs.aws.amazon.com/AmazonS3/latest/dev/LogFormat.html).
    
## 4. Open the Athena console

* If you see this warning, configure a result bucket to store Athena query results.
    * Create a new s3 bucket `aws-athena-query-results-<ACCOUNT_ID>-us-east-1`.

![step 4](./img/4_configure_result_bucket.png)

![step 4](./img/4_2_add_result_bucket_settings.png)

## 5. Execute the query to create a database

```
      create database s3_access_logs_db;
```

## 6. Execute the query to load your s3 log data into Athena

```
      CREATE EXTERNAL TABLE IF NOT EXISTS s3_access_logs_db.dto_logs(
         BucketOwner STRING,
         Bucket STRING,
         RequestDateTime STRING,
         RemoteIP STRING,
         Requester STRING,
         RequestID STRING,
         Operation STRING,
         Key STRING,
         RequestURI_operation STRING,
         RequestURI_key STRING,
         RequestURI_httpProtoversion STRING,
         HTTPstatus STRING,
         ErrorCode STRING,
         BytesSent BIGINT,
         ObjectSize BIGINT,
         TotalTime STRING,
         TurnAroundTime STRING,
         Referrer STRING,
         UserAgent STRING,
         VersionId STRING,
         HostId STRING,
         SigV STRING,
         CipherSuite STRING,
         AuthType STRING,
         EndPoint STRING,
         TLSVersion STRING
      ) 
      ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
      WITH SERDEPROPERTIES (
               'serialization.format' = '1', 'input.regex' = '([^ ]*) ([^ ]*) \\[(.*?)\\] ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) \\\"([^ ]*) ([^ ]*) (- |[^ ]*)\\\" (-|[0-9]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\"[^\"]*\") ([^ ]*)(?: ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*))?.*$' )
      LOCATION 's3://cedchan-s3-dto-logs/'

```
    
## 7. View the schema of your external table and preview a sample of the data

![step 7](./img/7_dto_logs_schema.png)

## 8.  Execute the query to group your s3 data transfers by region, user-agents, destination and data transfer size over this month

```
    SELECT remoteip,
         SUBSTR(useragent, 1, 15) as useragent,
         SUM(bytessent) AS uploadtotal_bytes,
         SUM(objectsize) AS downloadtotal_bytes,
         SUM(bytessent + objectsize) AS total_bytes
    FROM s3_access_logs_db.dto_logs
    WHERE bucket = 'YOUR_USERNAME-dto-logs'
            AND parse_datetime(requestdatetime,'dd/MMM/yyyy:HH:mm:ss Z') 
                BETWEEN parse_datetime('2019-11-01','yyyy-MM-dd') AND parse_datetime('2019-11-30','yyyy-MM-dd')
    GROUP BY  remoteip, useragent;
    
```
    
Other helpful filters include searching by `requesturi_operation = 'GET'` and `httpsstatus`
    
You can now view information such as the source of outgoing requests and size of downloaded data. 
    
![step 8](./img/8_2_results_2.png)

In this example, you can see that: 
* There is a significant file upload (2MB) via the S3 Console 
* There are significant file downloads from 2 hosts (my EC2 instances) with IPs 34.230.x.x and 35.153.x.x at 6GB and 0.49GB each - this is because I was executing a scrip using the `aws-cli` tool to download my S3 files.


## 9. As new logs are added to your audit bucket, you may need to update the Hive metadata to fetch the new logs in Athena.

Run `MSCK REPAIR TABLE s3_access_logs_db.dto_logs` and re-execute step 8.
    
