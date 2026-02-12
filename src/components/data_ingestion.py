import os
import sys
import pandas as pd
from src.exception import CustomException
from src.logger import logging
from dataclasses import dataclass

@dataclass
class DataIngestionConfig:
    raw_data_path: str = os.path.join('artifacts', "raw.csv")

class DataIngestion:
    def __init__(self):
        self.ingestion_config = DataIngestionConfig()

    def initiate_data_ingestion(self):
        logging.info("Starting Data Ingestion")
        try:
            movies = pd.read_csv('notebook/data/tmdb_5000_movies.csv')
            credits = pd.read_csv('notebook/data/tmdb_5000_credits.csv')
            
            df = movies.merge(credits, on='title')
            
            os.makedirs(os.path.dirname(self.ingestion_config.raw_data_path), exist_ok=True)
            df.to_csv(self.ingestion_config.raw_data_path, index=False)
            
            logging.info("Ingestion completed")
            return self.ingestion_config.raw_data_path
        except Exception as e:
            raise CustomException(e, sys)