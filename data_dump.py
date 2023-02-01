import pymongo
import pandas as pd
import json

#print(pd.__version__)

# Provide the mongodb localhost url to connect python to mongodb.
client = pymongo.MongoClient("mongodb://localhost:27017/neurolabDB")

#DATA_FILE_PATH = "/config/workspace/books.csv"
DATABASE_NAME = "mylib"
BOOK_DATA_FILE_PATH ='/config/workspace/book/books.csv'
USER_DATA_FILE_PATH ='/config/workspace/book/users.csv'
RATING_DATA_FILE_PATH ='/config/workspace/book/ratings.csv'

COLLECTION_NAME_BOOKS = "books"
COLLECTION_NAME_USERS = "users"
COLLECTION_NAME_RATINGS = "ratings"


if __name__=="__main__":

    books_df = pd.read_csv(BOOK_DATA_FILE_PATH,low_memory=False)
    users_df = pd.read_csv(USER_DATA_FILE_PATH,low_memory=False)
    ratings_df = pd.read_csv(RATING_DATA_FILE_PATH,low_memory=False)

    print(f"books rows and column: {books_df.shape}")
    print(f"users rows and column: {users_df.shape}")
    print(f"ratings rows and column: {ratings_df.shape}")

    books_df.reset_index(drop=True,inplace=True)
    users_df.reset_index(drop=True,inplace=True)
    ratings_df.reset_index(drop=True,inplace=True)

    json_record_books = list(json.loads(books_df.T.to_json()).values())
    json_record_users = list(json.loads(users_df.T.to_json()).values())
    json_record_ratings = list(json.loads(ratings_df.T.to_json()).values())

    #print(json_record[0:2])
    client[DATABASE_NAME][COLLECTION_NAME_BOOKS].insert_many(json_record_books) 
    client[DATABASE_NAME][COLLECTION_NAME_USERS].insert_many(json_record_users) 
    client[DATABASE_NAME][COLLECTION_NAME_RATINGS].insert_many(json_record_ratings) 



    '''
    df = pd.read_csv(DATA_FILE_PATH,low_memory=False)
    print(f"rows and column: {df.shape}")
    df.reset_index(drop=True,inplace=True)

    json_record = list(json.loads(df.T.to_json()).values())
    #print(json_record[0:2])
    client[DATABASE_NAME][COLLECTION_NAME].insert_many(json_record) '''