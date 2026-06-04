# Glints Data Role ETL

## The Architecture

I made a scraper with python using http-client library called 'wreq' (formerly 'rnet' blazing fast python http-client). 
The reason I use this package is because web-scraping youtuber called "John Watson Rooney" introduced it to me as the new bs4+requests way to scraping. 

## What the script do

This python scripts of mine will take all the detailed data from response of graphql API, from the role title, the estimated salary, the company name and location, and the job categories. Later after the program successfully run from GCP task/run, the scraped data will be store in big query dataset/table, and later it is ready to be analyzed. 

## The Business Insight for Optimization

The last update, I wanna know how much unique data I got from around 20k rows of data, and what I found is actually only about 20% of the data is unique, that means I overscrape, and should've made a longer scheduler time interval instead of doing it hourly, because not every hour there are hundreds new job posted in my specific role or title I searched (Data Analyst, Scientist, and Engineer).

## Tech Stack

![Gemini](https://img.shields.io/badge/Gemini_AI-8E75FF?style=flat&logo=googlegemini&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![Google Cloud](https://img.shields.io/badge/Google_Cloud-4285F4?style=flat&logo=google-cloud&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-2088FF?style=flat&logo=github-actions&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-150458?style=flat&logo=pandas&logoColor=white)

- **AI Co-Pilot / Engineering Assistant**: Gemini AI (Used on web for rapid prototyping, logic validation, and code structuring)
- **Scraping Architecture**: Python (`curl-cffi` / `Playwright` to pull raw target data)
- **Data Wrangling & Math**: Core Python & `Pandas` (Manual processing, regex cleaning, and statistical scoring logic)
- **Cloud Infrastructure**: Google Cloud Platform (Cloud Run serverless containers powered by your $300 credit)
- **Automation / Orchestration**: GitHub Actions (Scheduled CRON routines to trigger the pipeline daily)
