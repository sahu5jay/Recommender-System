import os 
from src.components.data_ingestion import DataIngestion 
from src.components.data_transformation import DataTransformation
from src.exception import CustomException
from src.logger import logging
# from src.components.model_training import ModelTrainer

if __name__ == '__main__':
    obj = DataIngestion()
    raw_data_path = obj.initiate_data_ingestion()

    transformation = DataTransformation()
    transformed_df = transformation.initiate_data_transformation(raw_data_path)

    # obj3 = ModelTrainer()
    # obj3.iniate_model_training(train_arr,test_arr)
