import ipywidgets as widgets
from ipywidgets import interact, SelectMultiple, fixed, Checkbox, IntRangeSlider, IntSlider, FloatSlider
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import json
from src.DataExtractor import DataExtractor


class DataVisualization():
    def __init__(self):
        self.data_extractor = DataExtractor()
    
    
    # A function used in order to plot the coordinates on the rink
    # It is a separate function from plot_game in order to be able to get the maximum number of games dinamically
    def play_visualization(self, game: dict):
        @interact(
            play_ID=IntSlider(min=1, max=len(game['liveData']['plays']['allPlays']), value=1, description='Play', continuous_update=True),
            )

        def plot_visualization_play(play_ID):
            play = self.data_extractor.get_play_by_ID(game , play_ID-1)
            img = plt.imread("../figures/nhl_rink.png")
            fig, ax = plt.subplots()
            ax.imshow(img, extent=[ -100, 100, -55, 55])
            coord = game['liveData']['plays']['allPlays'][play_ID-1]['coordinates']
            print('coordonnées : ',coord)
            if not coord: 
                print("no coord")
            else:
                plt.scatter(coord['x'], coord['y'])
            plt.title(game['liveData']['plays']['allPlays'][play_ID-1]['result']['description'])
            plt.show()
            
    def season_visualization(self, year: int):
        """
        
        Main function of our visualization. Takes the year as input, downloads the dictionary of the specified year. We use widgets.interact
        in order to create multiple sliders to select a specific game. Then we use widgets.interact a second time in order to be able to
        select a specific play of the game we have previously selected. 
        """ 
        
        entire_season = self.data_extractor.get_season_data(year)
        max_game = len(entire_season) - 105
        # year=IntSlider(min=2016, max=2020, value=1, description='Game', continuous_update=True)
        @interact(
        season_type=IntSlider(min=2, max=3, value=1, description='type', continuous_update=False),
        game_ID=IntSlider(min=1, max=max_game, value=1, description='Game ID', continuous_update=False),
        playoff_round = IntSlider(min=1, max=4, value=1, description='Round', continuous_update=False),
        matchup = IntSlider(min=1, max=8, value=1, description='MatchUp', continuous_update=False),
        games_num = IntSlider(min=1, max=7, value=1, description='game', continuous_update=False),
        )

        def plot_game( season_type, game_ID, playoff_round, matchup, games_num):
            if season_type == 2:
                game = self.data_extractor.get_game_from_dict(year, game_ID,  season_type, entire_season)
                home_goals = game['liveData']['linescore']['periods'][-1]['home']['goals']
                away_goals =game['liveData']['linescore']['periods'][-1]['away']['goals']
                home_sog = game['liveData']['linescore']['periods'][-1]['home']['shotsOnGoal']
                away_sog = game['liveData']['linescore']['periods'][-1]['away']['shotsOnGoal']
                Data = game['gameData']
                print(Data['datetime']['dateTime'])
                ID = self.data_extractor.build_game_ID(game_ID, year, season_type)
                print("Game ID :", ID, " ; ", Data['teams']['home']['abbreviation']," (home) VS",Data['teams']['away']['abbreviation'],"(away) " )
                print()
                Dict = {}
                Dict['teams'] = [game['gameData']['teams']['home']['abbreviation'], game['gameData']['teams']['away']['abbreviation']]
                Dict['Goals'] = [home_goals,away_goals]
                Dict['SoG'] = [home_sog,away_sog]
                #Dict['SO Goals'] = [0,0]
                #Dict['SO Attempts'] = [0,0]
                df = pd.DataFrame(Dict).T
                df.columns = ['Home', 'Away']
                print(df)
                self.play_visualization(game)

            if season_type == 3:
                try:
                    game = self.data_extractor.get_game3(year, season_type,playoff_round,matchup, games_num, entire_season)
                    Data = game['gameData']

                    if game['gameData']['status']['detailedState'] == 'Scheduled (Time TBD)' or game['gameData']['status']['detailedState'] == 'Scheduled' :
                        print('This game was not played ')
                    else : 
                        home_goals = game['liveData']['linescore']['periods'][2]['home']['goals']
                        away_goals =game['liveData']['linescore']['periods'][2]['away']['goals']
                        home_sog = game['liveData']['linescore']['periods'][2]['home']['shotsOnGoal']
                        away_sog = game['liveData']['linescore']['periods'][2]['away']['shotsOnGoal']
                        print(Data['datetime']['dateTime'])
                        ID = str(year) + "03" + str(playoff_round).zfill(2) + str(matchup) + str(games_num) 
                        print("Game ID :", ID, " ; ", Data['teams']['home']['abbreviation']," (home) VS",Data['teams']['away']['abbreviation'],"(away) " )
                        print()
                        Dict = {}
                        Dict['teams'] = [game['gameData']['teams']['home']['abbreviation'], game['gameData']['teams']['away']['abbreviation']]
                        Dict['Goals'] = [home_goals, away_goals]
                        Dict['SoG'] = [home_sog,away_sog]
                        #Dict['SO Goals'] = [0,0]
                        #Dict['SO Attempts'] = [0,0]
                        df = pd.DataFrame(Dict).T
                        df.columns = ['Home', 'Away']
                        print(df)
                        self.play_visualization(game)

                except KeyError as e:
                    print('No such game')
                    
    
    def 
