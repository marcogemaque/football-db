import os
import uuid
import psycopg2
import pandas as pd
from dotenv import load_dotenv

def load_credentials():
    """
    A function to load credentials
    from the dotenv.
    """
    load_dotenv("./.secrets/.env")
    username = os.environ["pg_username"]
    password = os.environ["pg_password"]
    database = os.environ["pg_database"]
    host = os.environ["pg_host"]
    return username, password, database, host

def load_data_into_postgres(
        row,table_name,cursor
    ):
    """
    Pandas apply method to insert rows of data into
    postgres.

    Parameters
    ------------
    row
        The pandas apply series.
    table_name
        Target table in postgres
    list_of_cols
        Columns in postgres by name
    list_of_value
        The values to be inserted
    """
    # uuid = row["uuid"]
    team_name = row["team_name"]
    alias = row["alias"]
    query = f"""
        INSERT INTO public.{table_name} (team_name,alias)
        VALUES ('{team_name}',ARRAY {alias})
    """
    print(f"INSERTED ROW --> ('{team_name}','ARRAY {alias})")
    cursor.execute(query)
    return row

def load_team_keys():
    """
    The first function to load teams with the UUIDs
    and the official name.
    """
    #read the file to create the UUIDs
    PATH = "./data/input/team_keys_load.csv"
    df = pd.read_csv(PATH)
    #add UUIDs as a new column to this dataframe
    df["uuid"] = df["country"].apply(lambda x: uuid.uuid4())
    #Get the credentials and add the data to postgres.
    username,password,database,host = load_credentials()
    conx = psycopg2.connect(user=username,
        password=password,host=host,dbname=database)
    with conx:
        cursor = conx.cursor()
        #insert the rows of data
        df = df.apply(load_data_into_postgres, axis=1, args=(
            "team_keys", cursor,
        ))

def load_team_alias():
    """
    The first function to load teams with their aliases.
    """
    #read the file to create the UUIDs
    PATH = "./data/input/team_aliases.csv"
    df = pd.read_csv(PATH)
    #add UUIDs as a new column to this dataframe
    # df["uuid"] = df["country"].apply(lambda x: uuid.uuid4())
    #Get the credentials and add the data to postgres.
    username,password,database,host = load_credentials()
    conx = psycopg2.connect(user=username,
        password=password,host=host,dbname=database)
    with conx:
        cursor = conx.cursor()
        #insert the rows of data
        df = df.apply(load_data_into_postgres, axis=1, args=(
            "teams_aliases", cursor)
        )

load_team_alias()