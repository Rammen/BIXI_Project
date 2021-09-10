from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from os import listdir
import os
import pandas as pd

"""
Take a folder with XML file and for each file:
    1. Check if this file exist in CSV in the destination folder
    2. Load the data with Pandas and save it into the new folder in CSV format
"""

class XmlToCsvOperator(BaseOperator):
    
    # UI colour in Apache Airflow
    ui_color = '#358140'
       
    @apply_defaults
    def __init__(self,
                 xml_folder='',
                 csv_folder='',
                 sub_document='',
                 *args, **kwargs):

        super(XmlToCsvOperator, self).__init__(*args, **kwargs)
        self.xml_folder = xml_folder
        self.csv_folder = csv_folder
        self.sub_document = sub_document
        

    def execute(self, context):

        # Create the folder where csv will be saved if it does not already exists
        if not os.path.exists(self.csv_folder):
            self.log.info(f"{self.csv_folder} does not exist. Will be created.")
            os.makedirs(self.csv_folder)

        # For each file in the folder
        # If CSV does not exists, it will take the XML and save it into CSV format
        for file in [f for f in listdir(self.xml_folder)]:
            self.log.info(f"Checking if {file} exists in CSV format in destination folder")
            self.log.info(f"{self.xml_folder}/{file[:-3]}csv")
            if os.path.isfile(f"{self.xml_folder}/{file[:-3]}csv"):
                self.log.info(f"CSV version of {file} alread exist in {self.csv_folder} folder")
                continue

            self.log.info(f"Creating {file} in CSV format in '{self.csv_folder}' folder")
#             weather = pd.read_xml(f"{self.xml_folder}/{file}", self.sub_document) # Requires Pandas version higher than 1.3.0
#             weather.to_csv(f"{self.csv_folder}/{file[:-3]}csv")