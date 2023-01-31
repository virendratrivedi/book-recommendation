import pymongo
import pandas as pd
import json

print(pd.__version__)

# Provide the mongodb localhost url to connect python to mongodb.
client = pymongo.MongoClient("mongodb://localhost:27017/neurolabDB")

DATA_FILE_PATH = "/config/workspace/Books.csv"
DATABASE_NAME = "mylib"
COLLECTION_NAME = "books"

if __name__=="__main__":
    df = pd.read_csv(DATA_FILE_PATH,low_memory=False)
    print(f"rows and column: {df.shape}")
    df.reset_index(drop=True,inplace=True)

    json_record = list(json.loads(df.T.to_json()).values())
    #print(json_record[0:2])
    client[DATABASE_NAME][COLLECTION_NAME].insert_many(json_record)