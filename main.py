from book.exception import BookException
from book.logger import logging
import os,sys
from book.enity import config_entity
from book.utils import get_collection_as_dataframe
from book.components.data_ingestion import DataIngestion
from book.components.data_transformation import DataTransformation



def test_looger_and_exception():
     try:
          result=10/0
          print(result)
     except Exception as e:
          raise BookException(e, sys)


if __name__ == "__main__":
     try: 
          training_pipeline_config = config_entity.TrainingPipelineConfig()
          data_ingestion_config = config_entity.DataIngestionConfig(training_pipeline_config=training_pipeline_config)
          #print(data_ingestion_config.to_dict())

          data_ingestion = DataIngestion(data_ingestion_config=data_ingestion_config)
          data_ingestion_artifact = data_ingestion.initiate_data_ingestion()
          #print(data_ingestion.initiate_data_ingestion())

          # Data Tranformation 
          data_transformation_config = config_entity.DataTransformationConfig(training_pipeline_config=training_pipeline_config)
          data_transformation = DataTransformation(data_transformation_config=data_transformation_config,
                             data_ingestion_artifact=data_ingestion_artifact)
          print(data_transformation.initiate_data_transformation())



          




          #mydf=get_collection_as_dataframe(database_name='mylib', collection_name='books')
          #print("testshape:",mydf.shape)




     except Exception as e:
          raise e






















'''
import pymongo

# Provide the mongodb localhost url to connect python to mongodb.
client = pymongo.MongoClient("mongodb://localhost:27017/neurolabDB")

# Database Name
dataBase = client["neurolabDB"]

# Collection  Name
collection = dataBase['Products']

# Sample data
d = {'companyName': 'iNeuron',
     'product': 'Affordable AI',
     'courseOffered': 'Machine Learning with Deployment'}

# Insert above records in the collection
rec = collection.insert_one(d)

# Lets Verify all the record at once present in the record with all the fields
all_record = collection.find()

# Printing all records present in the collection
for idx, record in enumerate(all_record):
     print(f"{idx}: {record}")
'''