import os,sys
from book.logger import logging
from book.exception import BookException
from datetime import datetime


FILE_NAME_BOOKS = "books.csv"
FILE_NAME_USERS = "users.csv"
FILE_NAME_RATINGS = "ratings.csv"
RATINGS_WITH_NAMES = "ratings_with_names.csv"
POPULAR_PKL_FILE= "popular.pkl"
SIMILARITY_SCORE_FILE="similarity.npz"



class TrainingPipelineConfig:
    try:
        def __init__(self):
            self.artifact_dir=os.path.join(os.getcwd(),"artifact",f"{datetime.now().strftime('%m%d%Y__%H%M%S')}")
    except Exception as e:
        raise BookException(e, sys)   

class DataIngestionConfig:
    def __init__(self,training_pipeline_config:TrainingPipelineConfig):
        try:
            self.database_name='mylib'
            self.collection_books='books'
            self.collection_users='users'
            self.collection_ratings='ratings'

            self.data_ingestion_dir = os.path.join(training_pipeline_config.artifact_dir,"data_ingestion")
            #self.feature_store_file_dir = os.path.join(self.data_ingestion_dir,"featue_store")
            self.feature_store_book_file_path=os.path.join(self.data_ingestion_dir,"feature_store",FILE_NAME_BOOKS)
            self.feature_store_users_file_path=os.path.join(self.data_ingestion_dir,"feature_store",FILE_NAME_USERS)
            self.feature_store_ratings_file_path=os.path.join(self.data_ingestion_dir,"feature_store",FILE_NAME_RATINGS)
            #self.books_file_path=os.path.join(self.feature_store_file_dir,FILE_NAME_BOOKS)
            #self.users_file_path=os.path.join(self.feature_store_file_dir,FILE_NAME_USERS)
            #self.ratings_file_path=os.path.join(self.feature_store_file_dir,FILE_NAME_RATINGS)

        except Exception as e:
            raise BookException(e, sys)

    def to_dict(self,)->dict:
        try:
            return self.__dict__
        except Exception  as e:
            raise SensorException(e,sys) 


class DataTransformationConfig:
    def __init__(self,training_pipeline_config:TrainingPipelineConfig):
        try:
            self.data_transformation_dir=os.path.join(training_pipeline_config.artifact_dir,"data_transformation")
            self.ratings_with_names_file_path=os.path.join(self.data_transformation_dir,RATINGS_WITH_NAMES)  
            self.popular_pkl_file_path = os.path.join(self.data_transformation_dir,"pickle_files",POPULAR_PKL_FILE) 
            #self.popular_pkl_dir = os.path.join(self.data_transformation_dir,"pickle_files") 
            #self.popular_pkl_file_path=os.path.join(popular_pkl_dir,POPULAR_PKL_FILE)
            
            
        except Exception as e:
            raise BookException(e, sys)

class ModelTrainingConfig:
    try:
        def __init__(self,training_pipeline_config:TrainingPipelineConfig):
            self.model_train_dir=os.path.join(training_pipeline_config.artifact_dir,"model_training")
            self.similarity_score_file_path=os.path.join(self.data_transformation_dir,SIMILARITY_SCORE_FILE)  
            

            
    except Exception as e:
        raise BookException(e, sys)

class ModelEvaluationConfig:...
class ModelPusherConfig:...