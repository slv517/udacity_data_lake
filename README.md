Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.


Project Description
In this project, you'll apply what you've learned on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. To complete the project, you will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3. You'll deploy this Spark process on a cluster using AWS.


Project Datasets
You'll be working with two datasets that reside in S3. Here are the S3 links for each:

Song data: s3://udacity-dend/song_data
Log data: s3://udacity-dend/log_data


Database Schema


Fact Table
1. songplays - records in log data associated with song plays i.e. records with page NextSong
   songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Dimension Tables
1. users - users in the app
   user_id, first_name, last_name, gender, level
2. songs - songs in music database
   song_id, title, artist_id, year, duration
3. artists - artists in music database
   artist_id, name, location, lattitude, longitude
4. time - timestamps of records in songplays broken down into specific units
   start_time, hour, day, week, month, year, weekday


Project Template
To get started with the project, go to the workspace on the next page, where you'll find the project template. You can work on your project with a smaller dataset found in the workspace, and then move on to the bigger dataset on AWS.

Alternatively, you can download the template files in the Resources tab in the classroom and work on this project on your local computer.

The project template includes three files:

- etl.py reads data from S3, processes that data using Spark, and writes them back to S3
- dl.cfgcontains your AWS credentials
- README.md provides discussion on your process and decisions
