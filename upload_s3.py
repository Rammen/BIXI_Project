from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowSkipException
import airflow.hooks.S3_hook
from os import listdir

"""
Upload To S3 Operator:
Take data from a local source (e.g. local folder) and upload the raw data into an S3 bucket
By default, this operator will skip a file if it is already in S3 
"""

class UploadToS3Operator(BaseOperator):
    
    # UI colour in Apache Airflow
    ui_color = '#358140'
    
    
    @apply_defaults
    def __init__(self,
                 s3_connection="S3_conn", # Connection to S3
                 foldername="", # Local Folder where the files are located
                 bucketname="", # Name of the bucket in S3 (must have been created prior)
                 replace=False, # If a file of the same name exists in S3, it will automatically be skipped
                 *args, **kwargs):

        super(UploadToS3Operator, self).__init__(*args, **kwargs)
        self.s3_connection = s3_connection
        self.foldername = foldername
        self.bucketname = bucketname
        self.replace = replace
        

    def execute(self, context):
            # Set credentials
            self.log.info("----> Setting S3 credentials")
            hook = airflow.hooks.S3_hook.S3Hook(self.s3_connection)

            files = [f for f in listdir(self.foldername)]
            uploaded_files = 0
            for filename in files:
                try: 
                    self.log.info(f"Uplodaing data from {filename} into S3")
                    hook.load_file(filename=f"{self.foldername}{filename}", key=filename, bucket_name=self.bucketname, replace=self.replace)
                    uploaded_files+=1
                    
                except:
                    self.log.info(f"File {filename} is already loaded in S3. Use replace=True when creating the operator if you want to replace this file")

            if uploaded_files > 0:
                self.log.info(f"Successfuly uploaded {uploaded_files}/{len(files)} files from {self.foldername} into S3")
            elif uploaded_files == 0:
                self.log.info(f"No files were loaded in S3, this step was skipped")
                raise AirflowSkipException