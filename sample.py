import os
import sys
from zipfile import ZipFile
from hate.logger import logging
from hate.exception import CustomException
from hate.configuration.gcloud_syncer import GCloudSync
from hate.entity.artifact_entity import DataIngestionArtifacts
from hate.entity.config_entity import DataIngestionConfig

class DataINgestion:
    def __init__(self, data_ingestion_config:DataIngestionConfig):
        self.data_ingestion_config = data_ingestion_config
        self.gcloud = GCloudSync()

    def get_data_from_cloud(self)->None:
        try:
            logging.info("Enterd the get_data_from_cloud method of data ingestion class")
            os.makedirs(self.data_ingestion_config.DATA_INGESTION_ARTIFACTS_DIR,exist_ok=True)

            self.gcloud.sync_folder_from_gcloud(self.data_ingestion_config.BUCKET_NAME,
                                                self.data_ingestion_config.ZIP_FILE_NAME,
                                                self.data_ingestion_config.DATA_INGESTION_ARTIFACTS_DIR)
            logging.info("exited the get_data_from_cloud method of Data ingestion class")
        except Exception as e:
            raise CustomException(e, sys) from e
    

    def unzip_and_clean(self):
        logging.info("entered the unzip and clean method of data ingestion class")
        try:
            with ZipFile(self.data_ingestion_config.ZIP_FILE_PATH,'r') as zip_ref:
                zip_ref.extractall(self.data_ingestion_config.ZIP_FILE_DIR)
            
            logging.info("exited the unzip and clean meethod of data ingestion class")

            return self.data_ingestion_config.DATA_ARTIFACTS_DIR, self.data_ingestion_config.NEW_DATA_ARTIFACTS_DIR
        except Exception as e:
            raise CustomException(e, sys) from e

    def initiate_data_ingestion(self)-> DataIngestionArtifacts:
        logging.info("entered  the initiate_ingestion_method of data ingestion class")

        try:
            self.get_data_from_cloud()
            logging.info("fetched the data from cloud")
            imbalance_data_file_path, raw_data_file_path = self.unzip_and_clean()
            logging.info("unzipped file and split into train and valid")

            data_ingestion_artifacts = DataIngestionArtifacts(
                imbalance_data_file_path = imbalance_data_file_path
                raw_data_file_path= raw_data_file_path
            )
            
            logging.info("exited the initiate_Data_ingestion method of data ingestion class")
            logging.info(f"data ingestion artifact:{data_ingestion_artifacts}")

            return data_ingestion_artifacts
        except Exception as e:
            raise CustomException(e,sys) from e

