import pandas as pd
import numpy as np
import os
import os as path
import json
import requests

class DataExtractor():
    def __init__(self):
        pass
    
    
    def get_game(self, path_to_file) -> dict:
        file = open(path_to_file, 'r', encoding='utf-8')
        json_str = file.read()
        season_dict = json.loads(json_str)
        file.close()
        return season_dict


    def clean_json(self, json_dict: dict) -> dict:
        live_data = json_dict["liveData"]
        shot_ID = 'SHOT'
        goal_ID = 'GOAL'

        play_data = live_data['plays']
        all_plays_data = np.array(play_data['allPlays'])

        def create_mask(play):
            return True if play['result']['eventTypeId'] == shot_ID or play['result']['eventTypeId'] == goal_ID else False

        vf = np.vectorize(create_mask)
        mask = vf(all_plays_data)

        return all_plays_data[mask]
    
    
    def create_panda_dataframe(self, np_array_data) -> pd.DataFrame:
        df = pd.DataFrame(columns = self.__generate_dataframe_column_names())
        for play_data in np_array_data:
            df = self.__add_play_data_to_dataframe(df, play_data)
        return df


    def __generate_dataframe_column_names(self)-> list:
        return ['about.periodTime', 'about.eventId', 'team.name', 'result.eventTypeId', 'coordinates.x', 'coordinates.y', 'players.0.player.fullName', 'players.1.player.fullName', 'result.secondaryType', 'result.strength.code', 'result.emptyNet']


    def __add_play_data_to_dataframe(self, df: pd.DataFrame, play_data: dict) -> dict:
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
