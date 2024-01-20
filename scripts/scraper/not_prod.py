import re
import requests
from bs4 import BeautifulSoup
import pandas as pd

def get_league_ranking(headers, URL):
    #make the request
    # URL = "https://www.transfermarkt.co.uk/superliga/startseite/wettbewerb/AR1N"
    page = requests.get(URL, headers=headers)
    #define the soup element to parse the result
    soup = BeautifulSoup(page.content, "html.parser")
    #find the IDs that we're looking for
    results = soup.find_all(id="yw5", attrs={"class":"grid-view"})
    class_for_table = "items",
    results = results.find_all("table", class_=class_for_table)
    full_table = [x.findChild("tbody") for x in results]
    only_rows = [x.findChildren("td") for x in full_table][0]
    teams_extracted_values = [x.text for x in only_rows if x.text != ""]
    # now transform it into a dataframe
    position_club = []
    club_names = []
    matches_played = []
    goal_diff = []
    pts_earned = []
    counter = 0
    for row in teams_extracted_values:
        if counter >= 5:
            counter = 0
        if counter == 0:
            position_club.append(row)
        elif counter == 1:
            club_names.append(row)
        elif counter == 2:
            matches_played.append(row)
        elif counter == 3:
            goal_diff.append(row)
        else:
            pts_earned.append(row)
        counter += 1
    df = pd.DataFrame()
    df["position"] = position_club
    df["club_names"] = club_names
    df["matches_played"] = matches_played
    df["goal_diff"] = goal_diff
    df["pts_earned"] = pts_earned
    #clean the table (specifically the CLUB NAME)
    df["club_names"] = df["club_names"].str.replace("\n","")
    #end of function
    return df