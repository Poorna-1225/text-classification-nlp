import os
import sys
from hate.logger import logging
from hate.exception import CustomException
from hate.components.data_ingestion import DataIngestion
from hate.entity.config_entity import DataIngestionConfig
from hate.entity.artifact_entity import DataIngestionArtifacts


class train_pipeline:
    def __init__(self):
        self.data_ingestion_config = DataIngestionConfig()


    def start_data_ingestion(self)-> DataIngestionArtifacts:
        logging.info("entered the start_data_ingestion method")
        try:
            logging.info("getting the data from cloud")
            data_ingestion = DataIngestion(data_ingestion_config = self.data_ingestion_config )
            data_ingestion_artifacts = data_ingestion.initiate_data_ingestion()
            logging.info("got the train and valid from gcloud")
            logging.info("exited from start_data_ingestion method")
            return data_ingestion_artifacts
        except Exception as e:
            raise CustomException(e, sys) from e
        
    def run_pipeline(self):
        logging.info("pipeline started")
        try:
            data_ingestion_artifacts = self.start_data_ingestion()
        except Exception as e:
            raise CustomException(e, sys) from e