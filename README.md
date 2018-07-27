# SMB Location Advisor - A Real-time Intelligent Tool for SMB Location Strategy

## Business Use Case
Shopping for the business location is usually the first challenge when SMB owners start their own business. There is huge need for them to quickly target an ideal location that can potentially grow their business without spending money on the consulting service for location strategy.
This tool aims to build up an intelligent tool to help SMB owners make data-driven site location decisions


## Data Source
### Yelp Open Source Dataset
This dataset includes information on reviews, users, businesses, checkin, photos and tips (5.79 GB uncompressed in json format, 6 json files including business.json, check-in.json, photos.json, review.json, tip.json and user.json)
### Income Dataset and Zip code Dataset 
1. American FactFinder under United States Census Bureau 
2. Gaslamp Media


## DE Challenges
### Ingest multiple data sources for joins
### Real-time streaming on:
1. NLP analysis, analyze the sentiment score of a user's comments on business, using Stanford CoreNLP wrapper for Apache Spark
2. Window aggregation counting of recent new user reviews on business
