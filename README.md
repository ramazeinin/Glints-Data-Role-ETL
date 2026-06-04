# Glints Data Role ETL

## The Architecture

I made a scraper with python using http-client library called 'wreq' (formerly 'rnet' blazing fast python http-client). 
The reason I use this package is because web-scraping youtuber called "John Watson Rooney" introduced it to me as the new bs4+requests way to scraping. 

## What the script do

This python scripts of mine will take all the detailed data from response of graphql API, from the role title, the estimated salary, the company name and location, and the job categories. Later after the program successfully run from GCP task/run, the scraped data will be store in big query dataset/table, and later it is ready to be analyzed. 

## The Business Insight for Optimization

The last update, I wanna know how much unique data I got from around 20k rows of data, and what I found is actually only about 20% of the data is unique, that means I overscrape, and should've made a longer scheduler time interval instead of doing it hourly, because not every hour there are hundreds new job posted in my specific role or title I searched (Data Analyst, Scientist, and Engineer).
