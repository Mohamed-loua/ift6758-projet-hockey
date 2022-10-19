import pandas as pd
import requests
import os
import os.path as path
import json


class StatsApiProxy:
    def __init__(self):
        pass
    
    
    def get_player_stats(self, year: int, player_type: str) -> pd.DataFrame:
        """

        Uses Pandas' built in HTML parser to scrape the tabular player statistics from
        https://www.hockey-reference.com/leagues/ . If the player played on multiple 
        teams in a single season, the individual team's statistics are discarded and
        the total ('TOT') statistics are retained (the multiple team names are discarded)

        Args:
            year (int): The first year of the season to retrieve, i.e. for the 2016-17
                season you'd put in 2016
            player_type (str): Either 'skaters' for forwards and defensemen, or 'goalies'
                for goaltenders.
        """

        if player_type not in ["skaters", "goalies"]:
            raise RuntimeError("'player_type' must be either 'skaters' or 'goalies'")

        url = f'https://www.hockey-reference.com/leagues/NHL_{year}_{player_type}.html'

        print(f"Retrieving data from '{url}'...")

        # Use Pandas' built in HTML parser to retrieve the tabular data from the web data
        # Uses BeautifulSoup4 in the background to do the heavylifting
        df = pd.read_html(url, header=1)[0]

        # get players which changed teams during a season
        players_multiple_teams = df[df['Tm'].isin(['TOT'])]

        # filter out players who played on multiple teams
        df = df[~df['Player'].isin(players_multiple_teams['Player'])]
        df = df[df['Player'] != "Player"]

        # add the aggregate rows
        df = df.append(players_multiple_teams, ignore_index=True)

        return df

    
    def fetch_live_data_for_years(self, years_to_fetch: list, path_to_directory: str, override: bool):
        self.__data_pipeline(years_to_fetch, path_to_directory, override)
        return
        
        
    def __data_pipeline(self, years_to_fetch: list, path_to_directory: str, override: bool):
        if path.exists(path_to_directory):
            print('File already exists, no download required!')
        else:
            for year in years_to_fetch:
                self.__download_games_for_season(year, path_to_directory, override)
                
        return
    
    
    def __download_games_for_season(self, year: int, path_to_directory: str, override: bool):
        """

        The first 4 digits identify the season of the game (ie. 2017 for the 2017-2018 season). The next 2 digits give the type of game,
         where 01 = preseason, 02 = regular season, 03 = playoffs, 04 = all-star. The final 4 digits identify the specific game number. 
         For regular season and preseason games, this ranges from 0001 to the number of games played. 
         (1271 for seasons with 31 teams (2017 and onwards) and 1230 for seasons with 30 teams). 
         For playoff games, the 2nd digit of the specific number gives the round of the playoffs, the 3rd digit specifies the matchup, 
         and the 4th digit specifies the game (out of 7).
        """

        try:
            print(f'Starting process to dowload all data for season {year}-{year+1}')
            #This is used to know when we reached the end of the games. The threshold can be changed if a lot of data is missing.
            nb_of_miss = 0
            threshold_of_miss = 5

            #build gameID for regular season
            game_id = self.__build_game_id(year, True)

            #Define variable for the bestOf
            best_of_in_playoff = 7

            #These two could be combined with time
            #build the directory path for regular season
            self.__check_for_directory_existence(path_to_directory+f'/Season{year}{year+1}'+f'/Regular{year}{year+1}')
            #build the directory path for playoff season
            self.__check_for_directory_existence(path_to_directory + f'/Season{year}{year + 1}' + f'/Playoff{year}{year + 1}') 

            season_dict = None

            #Get all the games for the regular season
            print(f'Regular season{year}-{year+1} :')
            while(nb_of_miss < threshold_of_miss):
                print(f'--GAMEID {game_id}')

                #Build the path to file using the gameID as the name and adding a sub directory for regular season
                path_to_file = path_to_directory+f'/Season{year}{year+1}'+f'/Regular{year}{year+1}'+f'/{game_id}.json'
                print(f'--Data will be saved at {path_to_file}')

                play_by_play, status_code = self.__download_play_by_play_for_game_id(game_id)
                if status_code == 200:
                    self.__json_to_separate_file(play_by_play, path_to_file, override)
                    season_dict = self.__json_to_single_file(play_by_play, game_id, season_dict, path_to_directory+f'/Season{year}{year+1}/season{year}{year+1}.json', override, False)
                    nb_of_miss = 0
                else:
                    nb_of_miss += 1
                game_id += 1

            #Get all the games for the playoffs
            nb_of_matchup = 0
            was_last_round = False
            game_id = self.__build_game_id(year, False)
            print(f'Playoff season{year}-{year + 1} :')
            
            while True:
                nb_of_miss = 0
                for game in range(best_of_in_playoff):
                    print(f'--GAMEID {game_id}')

                    # Build the path to file using the gameID as the name and adding a sub directory for playoffs
                    path_to_file = path_to_directory + f'/Season{year}{year + 1}' + f'/Playoff{year}{year + 1}' + f'/{game_id}.json'
                    print(f'--Data will be saved at {path_to_file}')

                    play_by_play, status_code = self.__download_play_by_play_for_game_id(game_id)
                    if status_code == 200:
                        self.__json_to_separate_file(play_by_play, path_to_file, override)
                        season_dict = self.__json_to_single_file(play_by_play, game_id, season_dict, path_to_directory + f'/Season{year}{year+1}/season{year}{year+1}.json', override, False)
                        was_last_round = False
                    else:
                        nb_of_miss += 1
                    game_id += 1
                if was_last_round:
                    break
                if nb_of_miss == best_of_in_playoff:
                    game_id = game_id + 100 - nb_of_matchup*10 - best_of_in_playoff #reset game_id for next round
                    was_last_round = True #tell that the last check didnt yield any information
                    nb_of_matchup = 0
                else:
                    game_id = game_id + 10 - best_of_in_playoff #reset game_id for next matchup
                    nb_of_matchup += 1
                
            self.__json_to_single_file('', '', season_dict, path_to_directory + f'/Season{year}{year+1}/season{year}{year+1}.json', override, True)
        
        except Exception as error:
            print(error)
            

    def __build_game_id(self, year: int, for_regular_season: bool) -> int:
        """

        The first 4 digits identify the season of the game (ie. 2017 for the 2017-2018 season). The next 2 digits give the type of game,
         where 01 = preseason, 02 = regular season, 03 = playoffs, 04 = all-star. The final 4 digits identify the specific game number. 
         For regular season and preseason games, this ranges from 0001 to the number of games played. 
         (1271 for seasons with 31 teams (2017 and onwards) and 1230 for seasons with 30 teams). 
         For playoff games, the 2nd digit of the specific number gives the round of the playoffs, the 3rd digit specifies the matchup, 
         and the 4th digit specifies the game (out of 7).
        """

        regularSeason = '02'
        regularFirstGame = '0001'

        playoffsSeason = '03'
        playoffsFirstGame = '0111'

        if for_regular_season:
            #example of a game id -- 2017021001
            return int((f'{year}{regularSeason}{regularFirstGame}'))
        if not for_regular_season :
            return int((f'{year}{playoffsSeason}{playoffsFirstGame}'))


    def __check_for_directory_existence(self, path_to_directory):
        try:
            if path.exists(path_to_directory) :
                print('Path to directory is valid')
                return True
            else:
                print('Path to directory is not valid, we will create a structure')
                os.makedirs(path_to_directory)
                print('The directory was created')
                return True
        except OSError as error:
            print(error)
            exit(0)


    def __json_to_separate_file(self, json: str, path_to_file: str, override: bool) -> bool:
        """

        Create a file a the location mention and write the json inside of it. The method will allow override of file if allowed.
        Args:
            json: json string of the data to be saved in a file
            path_to_file: path where the file will be created
            override: True to replace file if it exist, False to not override the file if it exist

        Returns: False if there was an error or if the file already exist and we do not wish to override. True otherwise
        """

        try:
            if path.exists(path_to_file) and not override:
                print('file exist and we are not overriding')
                return True
            if path.exists(path_to_file) and override:
                print(f'file exist but will be replace with current {path_to_file}')
            file = open(path_to_file, 'w', encoding='utf-8')
            file.write(json)
            file.close()
            return True
        except OSError as error:
            print(error)
            exit(0)


    def __json_to_single_file(self, json_game: str, game_id: int, season_dict: dict, path_to_file, override: bool, save_file: bool):
        """

        Append the json string to a singular file to create a single file that contains all the game of a singular season. This
        is done to avoid opening multiple time files making the process slower down the line. The initial step of dowloading all the
        might be slower but once the data is dowloaded, it will be easier to simply open a singular file and associate the content to
        a dictionnary rather then opening and closing multiple file to construct a dictonnary with all the data.

        json : a string in the form of a json to be added to the file
        path_to_file : string to indicate where to find or create the file
        override : True to replace the file if it exist, False to not replace the file and instead append to it.
        """

        if save_file:
            if path.exists(path_to_file):
                if override:
                    file = open(path_to_file, 'w', encoding='utf-8')
                    file.write(json.dumps(season_dict))
                    file.close()
                    return
            else:
                file = open(path_to_file, 'w', encoding='utf-8')
                file.write(json.dumps(season_dict))
                file.close()
                return
        else:
            if season_dict == None:
                if path.exists(path_to_file):
                    file = open(path_to_file, 'r', encoding='utf-8')
                    jsonStr = file.read()
                    season_dict = json.loads(jsonStr)
                    file.close()
                else:
                    season_dict = {}
            if not game_id in season_dict: #if the key isnt inside the dict we add the json to it
                season_dict[game_id] = json.loads(json_game)
            elif override:
                season_dict[game_id] = json.loads(json_game)
            return season_dict


    def __download_play_by_play_for_game_id(self, game_id: int) -> [str,int]:
        """

        The method will fetch a html page at the follwing url https://statsapi.web.nhl.com/api/v1/game/{game_id}/feed/live/ which contains
        all the information about a hockey game.

        Args:
            game_id: The game id to be fetched

        Returns: A json string which contains information about a specific Hockey game

        """
        try:
            json_play_by_play = requests.get(f'https://statsapi.web.nhl.com/api/v1/game/{game_id}/feed/live/')
            print(f"The request for game_id {game_id} returned a {json_play_by_play.status_code}")
            if json_play_by_play.status_code != 200:
                print(f'dowload for game_id : {game_id} failed, return code was {json_play_by_play.status_code}')
            return json_play_by_play.text, json_play_by_play.status_code
        except Exception as error:
            print(f'Error for game_id {game_id}')
            print(error)
            return '', 404
