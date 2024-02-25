#internal libs
from time import sleep
import os
#our created libs
from credentials_creator.security import create_credentials
from scraper.scraper import get_the_fixture_and_results, get_team_stats
from loader.to_cloud_storage import save_file_to_storage
from loader.load_to_dwh import load_file_to_table
from api import query_team_urls, query_team_aliases, refresh_materialized_view, query_competitions_urls
#third-party
import pandas as pd
import requests

def find_competition(row, headers, today):
    url = row['competition_url']
    season = row['season']
    competition_name = row['competition_name']
    #GET THE FIXTURES
    print("Scraping for FIXTURES.")
    fixtures = get_the_fixture_and_results(headers=headers, URL=url, season=season, competition_name=competition_name)
    print(f"Succeeded.")
    ##################
    #SAVE THE FIXTURE
    ##################
    #now merge it to the fixtures dataframe.
    teams_aliases = query_team_aliases()
    #iterate over the UUIDs, get the possible aliases and see if there's a match.
    #FIXME: There's definitely an improvement here. Maybe on the query side?
    fixtures_with_uuids = pd.merge(fixtures, teams_aliases, left_on=["home_team"], right_on=["team_name"], how='left')
    fixtures_with_uuids = fixtures_with_uuids.rename({"uuid":"home_team_uuid"}, axis=1)
    fixtures_with_uuids = fixtures_with_uuids.drop("team_name", axis=1)
    fixtures_with_uuids = pd.merge(fixtures_with_uuids, teams_aliases, left_on=["away_team"], right_on=["team_name"], how='left')
    fixtures_with_uuids = fixtures_with_uuids.rename({"uuid":"away_team_uuid"}, axis=1)
    fixtures_with_uuids = fixtures_with_uuids.drop("team_name", axis=1)
    #drop the columns we aren't going to use
    fixtures_with_uuids = fixtures_with_uuids.drop(["home_team","away_team"], axis=1)
    #save this file
    FILENAME = f"FIXTURES|football-db|{today}|{season}|{competition_name}.csv"
    #and now save this file to storage
    print(f"Saving the FIXTURE of TODAY...")
    save_file_to_storage(bucket_name='football-datalake', file_path_to_upload=FILENAME, object_to_upload=fixtures_with_uuids)
    print(f"Loading the FIXTURE of TODAY...")
    load_file_to_table(bucket_name='football-datalake', name_of_file=FILENAME, 
                   table_name="fixture")
    print("Succeeded.")

def apply_aliases_to_uuid(row, df_with_aliases):
    """
    Pandas Apply function to look for the aliases, see if there's a match,
    if there is, get the UUID
    """
    team_uuids = df_with_aliases["uuid"].tolist()
    team_aliases = df_with_aliases["alias"].tolist()
    #flatten the list
    team_aliases = [mini_list for sublist in team_aliases for mini_list in sublist]
    return row