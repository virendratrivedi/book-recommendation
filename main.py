from book.exception import BookException
from book.logger import logging
import os,sys
from book.utils import get_collection_as_dataframe


def test_looger_and_exception():
     try:
          result=10/0
          print(result)
     except Exception as e:
          raise BookException(e, sys)


if __name__ == "__main__":
     try:
          get_collection_as_dataframe(database_name='mylib', collection_name='books')

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