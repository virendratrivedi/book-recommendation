from book.logger import logging
from book.exception import BookException
from typing import Optional
import os,sys
from book import utils
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
import dill
import numpy as np
from book.enity import config_entity,artifact_entity

class ModelTraining:
        def __init__(self,model_training_config:config_entity.ModelTrainingConfig,
                     data_ingestion_artifact:artifact_entity.DataIngestionArtifact,
                     data_transformation_artifact:artifact_entity.DataTransformationArtifact):
            try:
                logging.info(f"{'>>'*20} Model Training {'<<'*20}")
                self.model_training_config=model_training_config
                self.data_ingestion_artifact=data_ingestion_artifact
                self.data_transformation_artifact=data_transformation_artifact
            except Exception as e:
                raise BookException(e, sys)

        def initiate_model_training(self,)->artifact_entity.ModelTrainingArtifact:
            try:
                ratings_with_names=pd.read_csv(self.data_transformation_artifact.ratings_with_names_path,low_memory=False)
                x=ratings_with_names.groupby('User-ID').count()['Book-Rating'] >200
                intelligent_user = x[x].index # boolean indexing 
                
                #Filerting based on user
                logging.info(f"Filerting based on user")
                filetered_rating = ratings_with_names[ratings_with_names['User-ID'].isin(intelligent_user)]

                # Filerting based on Books
                y = filetered_rating.groupby('Book-Title').count()['Book-Rating']>=50
                famous_books = y[y].index

                final_ratings = filetered_rating[filetered_rating['Book-Title'].isin(famous_books)]

                # Creating Pivot Table
                logging.info(f"creating pivot table")
                pivot_table = final_ratings.pivot_table(index='Book-Title',columns='User-ID',values='Book-Rating')
                logging.info(f"Rows and column of pivor table{pivot_table.shape}")
                pivot_table.fillna(0,inplace=True)

                # Calulting Distance using Cosine Similarity
                logging.info(f"Calculating Cosine simailarity")
                similarity_score = cosine_similarity(pivot_table)
                logging.info(f"similarity_score shape{similarity_score.shape}")

                










                







 
            except Exception as e:
                raise BookException(e, sys)    
            
    