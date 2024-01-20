import os
from dotenv import load_dotenv
import psycopg2
import pandas as pd

load_dotenv(".secrets/.env")

def connect_to_db():
    """
    A function to connect to the database.Returns a driver and cursor.

    Parameters
    -------------

    Returns
    -------------
    connection
        psycopg2.connect
    cursor
        psycopg2.cursor
    """
    server:str = os.environ["server"]
    user:str = os.environ["user"]
    password:str = os.environ["password"]
    db:str = os.environ["db"]
    connection = psycopg2.connect(
        host=server, dbname=db,
        user=user,password=password)
    cursor = connection.cursor()
    return connection, cursor

def query_team_urls():
    """
    Queries the URLS to search for each team (with UUID).
    """
    connection, cursor = connect_to_db()
    query = "SELECT * FROM scrape_urls left join team_keys using(uuid);"
    team_urls_to_query = pd.read_sql_query(query, con=connection)
    return team_urls_to_query

def delete_all_data_from_table(table_name:str):
    """
    A function to delete ALL ROWS from table_name.
    """
    connection, cursor = connect_to_db()
    print(f"Deleting data from table {table_name}...")
    query = f"delete from public.{table_name}"
    cursor.execute(query)
    connection.commit()
    print(f"Successful.")

def query_team_aliases():
    """
    Queries the ALIASES for each team (with UUID)
    """
    connection, cursor = connect_to_db()
    query = "SELECT * FROM teams_aliases left join team_keys using(uuid);"
    team_urls_to_query = pd.read_sql_query(query, con=connection)
    team_uuids = team_urls_to_query["uuid"].tolist()
    team_aliases = team_urls_to_query["alias"].tolist()
    #flatten the list
    team_aliases = [mini_list for sublist in team_aliases for mini_list in sublist]
    #create a new dataframe
    team_aliases_dataframe = pd.DataFrame()
    team_aliases_dataframe["team_name"] = team_aliases
    team_aliases_dataframe["uuid"] = team_uuids
    return team_aliases_dataframe

def refresh_materialized_view():
    """
    Queries to update the MATERIALIZED VIEW.
    """
    connection, cursor = connect_to_db()
    query = "REFRESH MATERIALIZED VIEW public.updated_ranking_table;"
    print("Refreshing MATERIALIZED VIEWS...")
    cursor.execute(query)
    connection.commit()
    print("Completed.")