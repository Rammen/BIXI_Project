# BIXI Project

**Technologies, libraries and frameworks I used:**

- Python, Jupyter Notebook
- Pandas
- SQL
- Apache Airflow
- Amazon Web Service (AWS):
	- S3
	- Redshift


![BIXI Logo](images/bixi_logo.png) 

![Udacity Logo](images/udacity_logo.png)

This project is part of the Capstone project for the [Data Engineering Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027). 

This project aims to better understand how weather (e.g. temperature, rain) impacts the number of biking rides completed by BIXI users, a sharing bike company located in Montréal, Canada. 

**Mains steps of this project:**
1.  First, I will develop a data engineering pipeline to load and transform the required data in a more usable data schema

2. Next, I will explore different analytics questions such as:
	- Is there a correlation between the temperature and amount of bike ride?
	- Does temperature has a bigger impact on workers (mon-fri during rush hour) than on leisure users (sat-sun during the afternoon)?

3. Finaly, I want to use machine learning to predict amount of BIXI trips based on temperature 


# Project Steps

## [Scope the Project, Gathering Data & Exploration](DataExploration.ipynb)

In this first and important step, I selected the dataset necessary to answer the questions defined above. Exploration of the dataset was completed using Pandas and some vizualisation tools (e.g. matplotlib, geopandas). 

### Datasets & Exploration
First, I took data from BIXI and found some complementary weather data matching the time period of BIXI activity. Since winter is full of snow and cold, BIXI only opperates its bikes from mid-april to the end of November based on snow conditions.

BIXI has many logs per day, thus, I only used data from 2020 to explore the datasets (e.g. variables, missing values). But for the full pipeline, I used data from 2018 to 2020.

**BIXI data**:
- [BIXI open-data](https://bixi.com/fr/donnees-ouvertes)
	- Yearly and monthly logs of trips completed
	- Location of all the BIXI stations

![BIXI stations across Montreal](images/data_exploration_stations.png)

**Weather**:
- [Montreal Climate - Historical Data](https://climate.weather.gc.ca/historical_data/search_historic_data_e.html) 
	- Hourly and daily weather in Montreal
    
    
## [Data Modeling & Pipeline Planification](DataModelingAndDataPipeline.ipynb)

In this second step, I started to plan how I would model the data to facilitate the analytics goals defined above. 

I choose a star-schema since I had one main information (BIXI logs) and many secondary datasets linked to these logs (weather, station, date) and not all analyses requires the same secondary datasets. More details can be found in the [Notebook](Data Modeling and Data Pipeline.ipynb). In the end, I skipped the date format since it was working pretty well without it and felt useless for the desired queries, but looking foward to adding new data to this schema!

![Schema of Data Modeling](images/data_modeling.png)

Based on this schema, I outlined the different steps I will take to create the data pipeline of this project. 

## [Pipeline Creation](BIXI_pipeline.py)

**Creation of the pipeline with Apache Airflow**
The following operators were created: 

	✓ Transforming XLM files (for weather) into CSV since redshift does not support easily XLM files
	
	✓ Transfering local files to S3
	
	✓ Creating empty tables (fact + dimensions + staging) in Redshift

	✓ Load data into staging table and transform them into fact and dimension table

	✓ Drop staging table once we are done using them

	✓ Quality checks (x2): no empty table and location of stations are in Montreal

![Data Engineering Pipeline Schema](images/Pipeline.PNG)


## Step 5: Capstone 

This latest step with dicussion about scalability, proof that the table have the good format in Redshift (and pipeline is working) as well as dictionnary can be found in the [Udacity Write Up Notebook](Udacity_WriteUp.ipynb). You can also find an analysis around daily usage based on temperature to further show that the pipeline is working.  

Interesingly we can see that COVID seems to have had a strong impact on the daily usage of BIXI. Indeed, the first summer after COVID started (2020) we can see that the number of rides drasticaly dropped (which is expected). The correlation between the temperature and number of rides is still very strong (0.708 in 2018-2019 and 0.756 in 2020). 

![Daily Bixi Usage](images/yearly_rides.png) 