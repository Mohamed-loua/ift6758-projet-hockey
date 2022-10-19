import pandas as pd
import numpy as np
import os
import os as path
import json
import requests
import warnings

warnings.filterwarnings("ignore")

class DataExtractor():
    def __init__(self):
        pass
    
    
    #function that takes the season to be downloaded and returns a dictionary containing the entirety of the games played during
    def get_season_data(self, year: int) -> dict:
        with open(f"../notebooks/hockey/Season{year}{year+1}/season{year}{year+1}.json", 'r') as j:
            entire_season = json.loads(j.read())
        return entire_season
    
    
    #Creation of game ID in order to find it in the dictionary
    def get_game_data(self, path_to_file) -> dict:
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

        #Is empty when a playoff game wasnt played
        if len(all_plays_data) == 0:
            return {}
        def create_mask(play):
            return True if play['result']['eventTypeId'] == shot_ID or play['result']['eventTypeId'] == goal_ID else False

        vf = np.vectorize(create_mask)
        mask = vf(all_plays_data)

        return all_plays_data[mask]
    
    
    def create_panda_dataframe(self, np_array_data) -> pd.DataFrame:
        if len(np_array_data) == 0:
            return None
        df = pd.DataFrame(columns = self.__generate_dataframe_column_names())
        for play_data in np_array_data:
            df = self.__add_play_data_to_dataframe(df, play_data)
        df.reset_index(drop=True, inplace=True)
        return df
    
    
    #Creation of game ID in order to find it in the dictionary
    def build_game_ID(self, game_ID: int, year: int, season_type: int) -> str:
        ID = str(year) + str(season_type).zfill(2) + str(game_ID).zfill(4)
        return ID
    
    
    #gets the game by looking up its ID in the dictionary which contains all the required data
    def get_game_from_dict(self, year: int, game: int,  season_type: int, entire_season: dict) -> dict :
        ID = self.build_game_ID(game, year, season_type)
        return entire_season[str(ID)]
    
    
    # gets the specified play (ID) for the game passed as a dictionary
    def get_play_by_ID(self, game : dict, ID : int) -> dict:
        play = game['liveData']['plays']['allPlays'][ID]
        return play
    
    def get_season_into_dataframe(self, path_to_file: str) -> pd.DataFrame:
        #Get the file that contains all the play of a seasons
        all_games_in_season = self.get_game_data(path_to_file)
        df_season = pd.DataFrame()
        for game in all_games_in_season:
            print(game)
            clean_game = self.clean_json(all_games_in_season.get(game))
            df_game = self.create_panda_dataframe(clean_game)
            df_season = df_season.append(df_game)
        return df_season
    #get the playoffs games
    def get_game3(self, year: int, type: int, round: int, matchup: int, games: int, entire_season: dict) -> dict :
        game_ID = year*10**6 + 3*10**4 + round*100 + matchup*10 + games
        return entire_season[str(game_ID)]


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
