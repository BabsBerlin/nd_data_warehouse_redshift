## Redshift Data Warehouse Project

### Introduction
For this project I build an **ETL pipeline for a database hosted on Redshift**, a Data Warehouse service in the AWS cloud. It was part of my *[Udacity Nanodegree in Data Engineering](https://www.udacity.com/course/data-engineer-nanodegree--nd027)*.

### The Task
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

### The Dataset
The database is based on two datasets. 
- **The song dataset** `(s3://udacity-dend/song_data)` is a subset of real data from the [Million Song Dataset](http://millionsongdataset.com/). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. 
- **The logfile dataset** `(s3://udacity-dend/log_data)` consists of log files in JSON format generated by an [Event Simulator](https://github.com/Interana/eventsim) based on the songs in the song dataset. It simulates activity logs from a music streaming app based on specified configurations. The log files are partitioned by year and month.
**The log data JSON path:** `(s3://udacity-dend/log_json_path.json)`

## The Database Schema

The database (db) is modeled after the star schema. [The Star Schema](https://en.wikipedia.org/wiki/Star_schema) separates business process data into facts, which hold the measurable, quantitative data about a business, and dimensions which are descriptive attributes related to fact data. 

For the Sparkify database we have the 'songplays' table as the fact table and the 'songs', 'artists', 'users', and 'time' tables as dimension tables.

## The project Files

- `create_table.py` accesses the sql queries to first drop any existing tables in the Sparkify db and then newly creates all tables 
- `etl.py` populates first the staging tables and then the analytics tables
- `sql_queries.py` contains all necessary SQL queries to create, populate, and drop the staging and analytics tables
- `create_delete_cluster.ipynb` contains code to create a new IAM role and Redshift cluster
- `check_database.ipynb` contains code to connect to the db and run SQL queries
- `dwh.cfg` contains the parameters to connect to the AWS workspace. For security reasons this file does provide the structure, but no real values. 

## Project Steps and how to run the project

1. To run the project you need to create a Redshift cluster first. To do this, you would have to fill out the necessary parameters in the `dwh.cfg` file and then run `create_delete_cluster.ipynb` steps 1 and 2. to create an IAM role and a new cluster. This code assumes that an IAM user with the necessary access rights is already in place in the AWS workspace.
2. Now run `create_tables.py`to create the db and all necessary tables.
3. To populate the tables you need to run `etl.py` which first fills the staging tables and from them the star schema tables.
4. After the database is set up and filled with data, you can run `check_database.ipynb`to connect to the database and run some SQL queries.
5. The close the project it is highly recommended to delete the cluster and the IAM role! You can find the code for this in `create_delete_cluster.ipynb` step 3a and 3b.
