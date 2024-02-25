#internal libs
from time import sleep
import os
#our created libs
from credentials_creator.security import create_credentials
from scraper.scraper import get_the_fixture_and_results, get_team_stats
from loader.to_cloud_storage import save_file_to_storage
from loader.load_to_dwh import load_file_to_table
from utils.app_utils import find_competition, apply_aliases_to_uuid
from api import query_team_urls, query_team_aliases, refresh_materialized_view, query_competitions_urls
#third-party
import pandas as pd
import requests

headers = requests.utils.default_headers()
headers.update({
'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0',
})
#define a variable TODAY to use throughout the code
today = pd.to_datetime('today').strftime('%Y-%m-%d')
#create JSON
create_credentials()
#common variables we will use
all_competitions = query_competitions_urls()
all_competitions = all_competitions.apply(find_competition,args=(headers,today,),axis=1)
#get URLs to scrape for the TEAM STATS
teams_urls_to_scrape = query_team_urls()
uuid = teams_urls_to_scrape["uuid"].tolist()
team_name = teams_urls_to_scrape["team_name"].tolist()
url_scrape = teams_urls_to_scrape["url"].tolist()
all_team_stats = pd.DataFrame()
for url in url_scrape:
    print("Scraping for TEAM STATS.")
    team_stat = get_team_stats(headers=headers, URL=url)
    team_stat["uuid"] = uuid[url_scrape.index(url)]
    team_stat["team_name"] = team_name[url_scrape.index(url)]
    all_team_stats = pd.concat([all_team_stats, team_stat])
    print(f"Succeeded.")
FILENAME = f"TEAM-STATS|football-db|{today}.csv"
save_file_to_storage(bucket_name='football-datalake', file_path_to_upload=FILENAME, object_to_upload=all_team_stats)
##################
#SAVE THE TEAM STATS
##################
print(f"Saving the TEAM STATS of TODAY...")
load_file_to_table(bucket_name='football-datalake', name_of_file=FILENAME, 
                   table_name="player_stats", drop_columns=True, cols_to_drop=["team_name"], 
                   complex_mode=False)
print("Succeeded.")
##################
#UPDATE MATERIALIZED VIEWS
##################
refresh_materialized_view()