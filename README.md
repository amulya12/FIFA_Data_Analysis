# FIFA_DataAnalytics - A complete end-to-end data engineering project on GCP.
An Simple End-to-End Data Engineering Project using the FIFA datasets and by utilizing tools like Google Cloud Platform (GCP), Airflow.
 
## Project Description
Developed an end-to-end dataengineering project to perform Exploratory data analysis on FIFA dataset.

## Procedure
  * Dowloaded the dataset from Kaggle.
  * Designed the GCP complete architecture.
  * Created a data pipeline using Airflow for processing the dataset, uploading it to the datalake, moving the data from the data lake to the data warehouse.
  * Transform the data in the data warehouse using ETL (extract, transform and load) tool and prepare it for analytics and visualisations.

## Architecture
![IMG_D4FAFB2DADF3-1](https://user-images.githubusercontent.com/30744874/163636162-fc8d821e-5f4a-4f56-b98f-1ecacb30bd30.jpeg)

## Dataset
Used FIFA 2017 official dataset for analysing data.  
FIFA17_official_data.csv - This dataset contains the following information like ID, Name, height, weight, Preferredfoot, etc.

You can download the data from [here.]( https://www.kaggle.com/code/soutanbasak/eda-on-fifa22-stats/data)

## Technologies

* Cloud : Google Cloud Platform (GCP)
* Workflow orchestration: Airflow
* Data Lake: Google Cloud Storage (GCS)
* Data Warehouse: Google BigQuery
* Transformation: Wrangle (Data Fusion)
* Visualisation: Google Data Studio

## ETL Workflow (Data fusion). 
<img width="866" alt="Screen Shot 2022-04-15 at 5 23 40 PM" src="https://user-images.githubusercontent.com/30744874/163649157-878b4ab6-00e2-4852-b0b0-9c8a023e762b.png">


## Visualization. 
Analysis on Preferred foot:  
<img width="1115" alt="Screen Shot 2022-04-15 at 5 11 56 PM" src="https://user-images.githubusercontent.com/30744874/163648483-87ce7a56-cf64-4031-bd02-7aef61a04bac.png">.  

Average age of all players in the dataset:  
<img width="269" alt="Screen Shot 2022-04-15 at 5 17 30 PM" src="https://user-images.githubusercontent.com/30744874/163648800-86a19a91-3e55-497d-a949-d093717da18e.png">. 

Top 10 players in with highest overall:  
<img width="310" alt="Screen Shot 2022-04-15 at 5 22 13 PM" src="https://user-images.githubusercontent.com/30744874/163649078-b28f8029-3eba-42ae-8b97-3559e0ba3e44.png">  

Top 10 players in with highest potential:  
<img width="312" alt="Screen Shot 2022-04-15 at 5 20 43 PM" src="https://user-images.githubusercontent.com/30744874/163648995-e8ce9790-8104-414b-ac37-89a0431f5ebd.png">





