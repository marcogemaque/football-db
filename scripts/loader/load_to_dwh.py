from api import delete_all_data_from_table, connect_to_db
from google.cloud import storage
import pandas as pd
import psycopg2
from psycopg2 import extras

def load_file_to_table(bucket_name:str, name_of_file, table_name, 
                       drop_columns:bool=False, cols_to_drop:list=None):
    """
    Function to load to table TEAM_STATS today's file.
    """
    #now read the data from the data lake
    storage_client = storage.Client.from_service_account_json("./.secrets/gcp_credentials.json")
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(name_of_file)
    blob.download_to_filename(f"./data/output/{name_of_file}")
    #now read that file and load it to the actual 
    df_to_load = pd.read_csv(f"./data/output/{name_of_file}")
    #delete all data currently existing in the TEAM STATS table
    delete_all_data_from_table(table_name)
    #and now load data from the dataframe into the table
    conn, cursor = connect_to_db()
    #drop any list of columns from the original file
    if drop_columns == True:
        df_to_load = df_to_load.drop(cols_to_drop, axis=1)
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df_to_load.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df_to_load.columns))
    # SQL quert to execute
    query  = "INSERT INTO %s(%s) VALUES %%s" % (f"public.{table_name}", cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
        print(f"Data loaded to table {table_name}.")
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_values() done")
    cursor.close()