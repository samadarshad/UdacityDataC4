
# Project - Data Lake

This project is an ETL pipeline for a data lake hosted on S3. We will load json data from S3, process the data into fact & dimension tables using Spark, and load them back into S3 as parquet files.

## Setup
1. A S3 bucket to store the output. Ensure S3 can be written into via the EMR role. e.g.:
   ```python
        {
            "Sid": "Stmt1660907761447",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::324941539183:role/EMR_EC2_DefaultRole"
            },
            "Action": "s3:*",
            "Resource": "arn:aws:s3:::spark-dev-324941539183"
        }


2. An EMR cluster with Spark. Use the default role above for the EC2. Enable keypair.

3. Ensure inbound SSH on Master node.

4. Confirm ssh connection:
`ssh -i "udacity_data_eng.pem" hadoop@ec2-18-132-1-161.eu-west-2.compute.amazonaws.com`

5. Transfer etl.py and etl_functions.py to master node:
`scp -i "udacity_data_eng.pem" etl.py hadoop@ec2-18-132-1-161.eu-west-2.compute.amazonaws.com:~/`
`scp -i "udacity_data_eng.pem" etl_functions.py hadoop@ec2-18-132-1-161.eu-west-2.compute.amazonaws.com:~/`
   
6. Run the application
`spark-submit etl.py --master yarn --py-files etl_functions.py`
   
Note that this will usually take a long time as it performs an API call for each file in S3 (song_data has approx 14,000 files, log_data 31 files).
Therefore we have limited it to a subset of data that exists in /song_data/A/A/*.
This would have been resolved by using the s3a:// protocol.
https://stackoverflow.com/questions/30385981/how-to-access-s3a-files-from-apache-spark