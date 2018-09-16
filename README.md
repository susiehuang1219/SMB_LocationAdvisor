# SMB LocationIQ - A scalable machine learning pipeline for SMB Location Strategy

## Business Use Case
Shopping for the business location is usually the first challenge when SMB owners start their own business. There is huge need for them to quickly target an ideal location that can potentially grow their business without spending money on the consulting service for location strategy. This tool aims to build up an intelligent tool to help SMB owners make data-driven site location decisions

## Demo
### Demo Video
[Demo Video](https://www.youtube.com/watch?v=9LDvZoIvAGE)
### Demo Slides
[Demo Slides](http://bit.ly/LocationIQ-slides)

## Data Source
### Yelp Open Source Dataset
This dataset includes information on reviews, users, businesses, checkin, photos and tips (6 json files including business.json, check-in.json, photos.json, review.json, tip.json and user.json)
### Income Dataset and Zip code Dataset 
1. American FactFinder under United States Census Bureau 
2. Gaslamp Media


## DE Challenges
1. Normalize different dimensions from the data input and compute an aggregated score in the aspects of Accessibility, Availability and Sentiment.
2. Implement both batch and streaming processing in the pipeline, where structured streaming with dataframe structure is still an experimental API in Spark.

## Cluster Configurations
1. Spark cluster: 4 m4.large instance 8G memory, 100G disk Ubuntu 16
2. Kafka cluster: 3 m4.large instance 8G memory, 100G disk Ubuntu 16
3. Spark Streaming cluster: 4 m4.large instance 8G memory, 100G disk Ubuntu 16
4. PostgreSQL cluster: 1 m4.large instance 8G memory, 100G disk Ubuntu 16

