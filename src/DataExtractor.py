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
    
    
    def get_season_data_for_team(self, year: int, team_id: int) -> dict:
        season_data = self.__get_season_data(year)
        
        team_dict = {}
        for game_id in season_data:
        # print(season_data[game_id].keys())
            team_1 = season_data[game_id]['gameData']['teams']['away']['id']
            team_2 = season_data[game_id]['gameData']['teams']['home']['id']
            
            if team_1 == team_id: # could be regrouped
                team_dict[game_id] = season_data[game_id]
                continue
            elif team_2 == team_id:
                team_dict[game_id] = season_data[game_id]
                continue
        return team_dict
    
    
    #function that takes the season to be downloaded and returns a dictionary containing the entirety of the games played during
    def __get_season_data(self, year: int) -> dict:
        with open(f"../notebooks/hockey/Season{year}{year+1}/season{year}{year+1}.json", 'r') as j:
            entire_season = json.loads(j.read())
        return entire_season
    
    
    # get all shots of one specific team 
    def get_shots(self, data: dict, team_id: int) -> np.array:
        shots = []
        for game_id in data:
            for play in data[game_id]['liveData']['plays']['allPlays']:
                try:
                    if play['result']['event'] == "Shot":
                        if play['team']['id'] == team_id:
                            if play['coordinates']['x'] is not None:
                                x = play['coordinates']['x']
                                y = play['coordinates']['y']
                                shots.append([x,y])
                except Exception as e:
                    print(e)
        return np.array(shots)
    
    
    # get total time played of one specific team 
    def get_time_played_from_team_season_data(self, data: dict) -> np.array:
        time_played = []
        for game_id in data:
            time_played.append(60)
        return np.array(time_played)
    
    
    #Creation of game ID in order to find it in the dictionary
    def build_game_ID(self, game_ID: int, year: int, season_type: int) -> str:
        ID = str(year) + str(season_type).zfill(2) + str(game_ID).zfill(4)
        return ID
    
    
    #gets the game by looking up its ID in the dictionary which contains all the required data
    def get_game_from_dict(self, year: int, game: int,  season_type: int, entire_season: dict) -> dict :
        ID = self.build_game_ID(game, year, season_type)
        return entire_season[str(ID)]
    
    
    #get the playoffs games
    def get_game3(self, year: int, type: int, round: int, matchup: int, games: int, entire_season: dict) -> dict :
        game_ID = year*10**6 + 3*10**4 + round*100 + matchup*10 + games
        return entire_season[str(game_ID)]
    
    
    # gets the specified play (ID) for the game passed as a dictionary
    def get_play_by_ID(self, game : dict, ID : int) -> dict:
        play = game['liveData']['plays']['allPlays'][ID]
        return play
    
    
    
    
    
    
    
    
    
    
    
    #Get the file that contains all the play of a season
    def get_season_into_dataframe(self, path_to_file: str) -> pd.DataFrame:
        all_games_in_season = self.__get_game_data(path_to_file)
        df_season = pd.DataFrame()
        
        for game in all_games_in_season:
            game_pk, clean_game = self.__clean_json(all_games_in_season.get(game))
            df_game = self.__create_panda_dataframe_for_one_game(game_pk, clean_game)
            df_season = df_season.append(df_game)
            df_season.reset_index(drop=True, inplace=True)
        return df_season
    
    
    # get all shots of one specific team 
    def get_team_shots_from_dataframe(self, df: pd.DataFrame, team_id: int) -> np.array:
        df.rename(columns={
            'team.id': 'teamID', 
            'coordinates.x': 'coordinatesX',
            'coordinates.y': 'coordinatesY'
            }, inplace=True)
        
        df = df[df.teamID == team_id]
        df = df.loc[:, ['coordinatesX', 'coordinatesY']]
        
        df = df.dropna()
        return np.array(df['coordinatesX']), np.array(df['coordinatesY'])
    
    
    # get total time played of one specific team 
    def get_time_played_from_team_season_dataframe(self, df: pd.DataFrame, team_id: int) -> np.array:
        time_played = []
        df.rename(columns={
            'team.id': 'teamID', 
            'coordinates.x': 'coordinatesX',
            'coordinates.y': 'coordinatesY'
            }, inplace=True)
        
        df = df[df.teamID == team_id]
        count_season_games = df['gamePk'].nunique()
        
        for i in range(count_season_games):
            time_played.append(60)
        return np.array(time_played)
    
    
    def __get_game_data(self, path_to_file) -> dict:
        file = open(path_to_file, 'r', encoding='utf-8')
        json_str = file.read()
        season_dict = json.loads(json_str)
        file.close()
        return season_dict


    def __clean_json(self, json_dict: dict) -> (str, dict):
        game_pk = json_dict['gamePk']
        live_data = json_dict['liveData']
        shot_ID = 'SHOT'
        goal_ID = 'GOAL'

        play_data = live_data['plays']
        all_plays_data = np.array(play_data['allPlays'])

        #Is empty when a playoff game wasnt played
        if len(all_plays_data) == 0:
            return game_pk, {}
        
        def create_mask(play):
            return True if play['result']['eventTypeId'] == shot_ID or play['result']['eventTypeId'] == goal_ID else False

        vf = np.vectorize(create_mask)
        mask = vf(all_plays_data)

        return game_pk, all_plays_data[mask]

    
    def __create_panda_dataframe_for_one_game(self, game_pk: str, all_play_data: dict) -> pd.DataFrame:
        if len(all_play_data) == 0:
            return None
        rows_dict = []
        
        for play_data in all_play_data:
            rows_dict.append(self.__extract_play_data_from_dict(game_pk, play_data))
            
        df = pd.DataFrame(rows_dict)
        return df
    

    def __generate_dataframe_column_names(self)-> list:
        return ['about.periodTime', 'about.eventId', 'team.name', 'team.id', 'result.eventTypeId', 'coordinates.x', 'coordinates.y', 'players.0.player.fullName', 'players.1.player.fullName', 'result.secondaryType', 'result.strength.code', 'result.emptyNet']


    def __extract_play_data_from_dict(self, game_pk: str, full_play_data: dict) -> dict:
        def extract_path_from_column_name(column_name: str) -> list:
            return column_name.split(".")
    
        def extract_value_from_path(path: list, full_play_data: dict):
            result = full_play_data
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

        for column in self.__generate_dataframe_column_names():
            path = extract_path_from_column_name(column)
            value = extract_value_from_path(path, full_play_data)
            new_dict[column] = value
        new_dict['gamePk'] = game_pk
        return new_dict
