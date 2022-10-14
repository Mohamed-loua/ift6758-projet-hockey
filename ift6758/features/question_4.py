import pandas as pd
import numpy as np
import os
import os as path
import json
import requests


def get_game(pathToFile) -> dict:
    file = open(pathToFile, 'r', encoding='utf-8')
    jsonStr = file.read()
    seasonDict = json.loads(jsonStr)
    file.close()
    return seasonDict


def clean_json(jsonDict: dict) -> dict:
    live_data = jsonDict["liveData"]
    shot_ID = 'SHOT'
    goal_ID = 'GOAL'

    play_data = live_data['plays']
    allplays_data = np.array(play_data['allPlays'])

    def create_mask(play):
        return True if play['result']['eventTypeId'] == shot_ID or play['result']['eventTypeId'] == goal_ID else False

    vf = np.vectorize(create_mask)
    mask = vf(allplays_data)

    return allplays_data[mask]


def get_play_type():
    json_playType = requests.get('https://statsapi.web.nhl.com/api/v1/playTypes')
    return json_playType


def generate_dataframe_column_names()-> list:
    return ['about.periodTime', 'about.eventId', 'team.name', 'result.eventTypeId', 'coordinates.x', 'coordinates.y', 'players.0.player.fullName', 'players.1.player.fullName', 'result.secondaryType', 'result.strength.code', 'result.emptyNet']


def add_play_data_to_dataframe(df: pd.DataFrame, play_data: dict) -> dict:
    def extract_path_from_column_name(column_name: str) -> list:
        return column_name.split(".")
    
    
    def extract_value_from_path(path: list, play_data: dict):
        result = play_data
        for i in range(len(path)):
            try:
                if path[i] == str(0):
                    result = result[0]
                elif path[i] == str(1):
                    result = result[len(result) - 1]
                else:
                    result = result[path[i]]
            except:
                return None
        return result 
    
    
    new_dict = {}
    
    for column in df.columns:
        path = extract_path_from_column_name(column)
        value = extract_value_from_path(path, play_data)
        new_dict[column] = value
    
    new_row_df = pd.DataFrame([new_dict])
    return df.append(new_row_df, )

def createPdDataframe(np_array_data) -> pd.DataFrame:
    df = pd.DataFrame(columns = generate_dataframe_column_names())
    for play_data in np_array_data:
        df = add_play_data_to_dataframe(df, play_data)
    return df


data = get_game('./notebooks/hockey/Season20172018/Regular20172018/2017020001.json')
data = clean_json(data)
pd_data = createPdDataframe(data).reset_index()
print(pd_data)

print("HI")