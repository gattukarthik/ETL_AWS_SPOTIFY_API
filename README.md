## Introduction
In this project we will build a ETL pipeline using the Spotify API on AWS. We will retrieve data from the Spotify API, transform it to a desired format and load it into an AWS data store.

## Architecture Design

## Services used
**S3 (Simple Storage Service)**: Amazon S3 (Simple Storage Service) is a highly scalable object storage storage that can store and retrieve any amount of data from anywhere. It is commonly used to store and distribute large media files, data backups and static website files.
**AWS Lambda**: AWS Lambda is a serverless computing service that lets you run your code without managing servers. You can use lambda to run code in response to events like changes in S3, DynamoDB or other AWS services.
**Cloud Watch**: Amazon Cloud Watch is a monitoring service for AWS resources and the applications you run on them. You can use Cloud Watch to collect and track metrics, collect and monitor log files and for setting alarms.
**Glue Crawler**: AWS Glue Crawler is a fully managed service that automatically crawles your data sources, identifies data formats and infers schemas to create an AWS Glue Data Catalog.
**Data Catalog**: AWS Glue Data Catalog is a fully managed metadata repository that makes it easy to discover and manage data in AWS. You can use the Glue Data Catalog with other AWS services such as Athena.
**Amazon Athena**: Amazon Athena is an interactive query services that makes it easy to analyze data in Amazon S3 using standard SQL. You can use Athena to analyze data in your Glue Data Catalog or in other S3 buckets.

## Project Execution Flow
Extract Data from API -> Lambda Trigger (every 1 hour) -> run extract code -> store raw data -> trigger transformation function -> transform data and load it -> query data using Athena.

## Steps Involved
**Integrating with Spotify API and Extracting Data**:Extract the unstructured data from Spotify's API using proper authentication methods for further processing.
**Spotify API Integration**: Set up the Spotify API to extract albums, artists, and tracks, then store the raw data locally in the data/raw/ folder. This step ensures proper data retrieval for further processing.
**Deploy Code on AWS Lambda**: Create and deploy a Lambda function that uses spotipy and boto3 to extract Spotify data and write it to an S3 bucket. This automates the data extraction process in a cloud environment.
**Add Trigger for Automatic Extraction**: Set up AWS CloudWatch Events to trigger the Lambda function automatically at regular intervals, such as daily, for continuous data extraction. This ensures the pipeline runs on schedule without manual intervention.
**Write Transformation Function**: Develop Python transformation functions using Pandas to clean and format raw data, ensuring it's ready for analysis. Save the transformed data for further use or upload to the cloud.
**Automate Transformation with S3 Trigger**: Configure S3 event notifications to trigger a Lambda function whenever new raw data is uploaded, automating the transformation process. This ensures data is processed as soon as it's available.
**Store Files on S3**: Organize raw and processed data in structured S3 directories (raw_data/, processed_data/), maintaining clear version control and accessibility. Implement appropriate permissions for secure data storage.
**Build Analytics with Glue and Athena**: Set up a Glue Crawler to catalog the S3 data and enable SQL-based querying using Amazon Athena. This allows for efficient analytics and reporting on the processed data.

## Conclusion
This project builds an automated, end-to-end data pipeline using the Spotify API, AWS Lambda, and S3, ensuring efficient data extraction, transformation, and storage. By leveraging Glue and Athena, it enables powerful analytics and querying of the processed data.
