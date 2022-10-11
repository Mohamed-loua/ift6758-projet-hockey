import pandas as pd
import requests
import os
import os.path as path
import json

def get_player_stats(year: int, player_type: str) -> pd.DataFrame:
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

def jsonToSeperateFile(json: str, pathToFile: str, override: bool) -> bool:
    """
    Create a file a the location mention and write the json inside of it. The method will allow override of file if allowed.
    Args:
        json: json string of the data to be saved in a file
        pathToFile: path where the file will be created
        override: True to replace file if it exist, False to not override the file if it exist

    Returns: False if there was an error or if the file already exist and we do not wish to override. True otherwise

    """
    try:
        if path.exists(pathToFile) and not override:
            print('file exist and we are not overriding')
            return True
        if path.exists(pathToFile) and override:
            print(f'file exist but will be replace with current {pathToFile}')
        file = open(pathToFile, 'w', encoding='utf-8')
        file.write(json)
        file.close()
        return True
    except OSError as error:
        print(error)
        exit(0)

def jsonToSingleFile(jsonGame: str, gameid:int, seasonDict:dict,pathToFile, override:bool, saveFile:bool):
    """
    Append the json string to a singular file to create a single file that contains all the game of a singular season. This
    is done to avoid opening multiple time files making the process slower down the line. The initial step of dowloading all the
    might be slower but once the data is dowloaded, it will be easier to simply open a singular file and associate the content to
    a dictionnary rather then opening and closing multiple file to construct a dictonnary with all the data.

    json : a string in the form of a json to be added to the file
    pathToFile : string to indicate where to find or create the file
    override : True to replace the file if it exist, False to not replace the file and instead append to it.
    """
    if saveFile:
        if path.exists(pathToFile):
            if override:
                file = open(pathToFile, 'w', encoding='utf-8')
                file.write(json.dumps(seasonDict))
                file.close()
                return
        else:
            file = open(pathToFile, 'w', encoding='utf-8')
            file.write(json.dumps(seasonDict))
            file.close()
            return
    else:
        if seasonDict == None:
            if path.exists(pathToFile):
                file = open(pathToFile, 'r', encoding='utf-8')
                jsonStr = file.read()
                seasonDict = json.loads(jsonStr)
                file.close()
            else:
                seasonDict = {}
        if not gameid in seasonDict: #if the key isnt inside the dict we add the json to it
            seasonDict[gameid] = json.loads(jsonGame)
        elif override:
            seasonDict[gameid] = json.loads(jsonGame)
        return seasonDict






    pass

def dowload_play_by_play_for_game_id(gameid: int) -> [str,int]:
    """
    The method will fetch a html page at the follwing url https://statsapi.web.nhl.com/api/v1/game/{gameid}/feed/live/ which contains
    all the information about a hockey game.

    Args:
        gameid: The game id to be fetched

    Returns: A json string which contains information about a specific Hockey game

    """
    try:
        json_play_by_play = requests.get(f'https://statsapi.web.nhl.com/api/v1/game/{gameid}/feed/live/')
        print(f"The request for gameid {gameid} returned a {json_play_by_play.status_code}")
        if json_play_by_play.status_code != 200:
            print(f'dowload for gameid : {gameid} failed, return code was {json_play_by_play.status_code}')
        return json_play_by_play.text, json_play_by_play.status_code
    except Exception as error:
        print(f'Error for gameid {gameid}')
        print(error)
        return '', 404

"""
The first 4 digits identify the season of the game (ie. 2017 for the 2017-2018 season). The next 2 digits give the type of game,
 where 01 = preseason, 02 = regular season, 03 = playoffs, 04 = all-star. The final 4 digits identify the specific game number. 
 For regular season and preseason games, this ranges from 0001 to the number of games played. 
 (1271 for seasons with 31 teams (2017 and onwards) and 1230 for seasons with 30 teams). 
 For playoff games, the 2nd digit of the specific number gives the round of the playoffs, the 3rd digit specifies the matchup, 
 and the 4th digit specifies the game (out of 7).
"""
def dowload_games_for_season(year: int, pathToDirectory, override: bool):
    try:
        print(f'Starting process to dowload all data for season {year}-{year+1}')
        #This is use to know when we reached the end of the games the threshold can be change if alot of data is missing
        nbOfMiss = 0
        thresholdOfMiss = 5

        #build gameID for regular season
        gameid = build_game_id(year, True)

        #Define variable for the bestOf
        bestOfInPlayoff = 7

        #These two could be combine with time
        checkForDirectoryExistence(pathToDirectory+f'/Season{year}{year+1}'+f'/Regular{year}{year+1}') #build the directory path for regular season
        checkForDirectoryExistence(pathToDirectory + f'/Season{year}{year + 1}' + f'/Playoff{year}{year + 1}')#build the directory path for playoff season

        seasonDict = None

        #Get all the games for the regular season
        print(f'Regular season{year}-{year+1} :')
        while(nbOfMiss < thresholdOfMiss):
            print(f'--GAMEID {gameid}')

            #Builds the path to file usin the gameID as the name and adding a sub directory for regular season
            pathToFile = pathToDirectory+f'/Season{year}{year+1}'+f'/Regular{year}{year+1}'+f'/{gameid}.json'
            print(f'--Data will be saved at {pathToFile}')

            play_by_play, status_code = dowload_play_by_play_for_game_id(gameid)
            if status_code == 200:
                jsonToSeperateFile(play_by_play, pathToFile, override)
                seasonDict = jsonToSingleFile(play_by_play, gameid, seasonDict, pathToDirectory+f'/Season{year}{year+1}/season{year}{year+1}.json', override, False)
                nbOfMiss = 0
            else:
                nbOfMiss += 1
            gameid += 1

        #Get all the games for the playoffs
        nbOfMatchup = 0
        wasLastRound = False
        gameid = build_game_id(year, False)
        print(f'Playoff season{year}-{year + 1} :')
        while True:
            nbOfMiss = 0
            for game in range(bestOfInPlayoff):
                print(f'--GAMEID {gameid}')

                # Builds the path to file usin the gameID as the name and adding a sub directory for regular season
                pathToFile = pathToDirectory + f'/Season{year}{year + 1}' + f'/Playoff{year}{year + 1}' + f'/{gameid}.json'
                print(f'--Data will be saved at {pathToFile}')

                play_by_play, status_code = dowload_play_by_play_for_game_id(gameid)
                if status_code == 200:
                    jsonToSeperateFile(play_by_play, pathToFile, override)
                    seasonDict = jsonToSingleFile(play_by_play, gameid, seasonDict, pathToDirectory + f'/Season{year}{year+1}/season{year}{year+1}.json', override, False)
                    wasLastRound = False
                else:
                    nbOfMiss += 1
                gameid += 1
            if wasLastRound:
                break
            if nbOfMiss == bestOfInPlayoff:
                gameid = gameid + 100 - nbOfMatchup*10 - bestOfInPlayoff #reset gameid for next round
                wasLastRound = True #tell that the last check didnt yield any information
                nbOfMatchup = 0
            else:
                gameid = gameid + 10 - bestOfInPlayoff #reset gameid for next matchup
                nbOfMatchup += 1
        jsonToSingleFile('', '', seasonDict, pathToDirectory + f'/Season{year}{year+1}/season{year}{year+1}.json', override, True)
    except Exception as error:
        print(error)
    pass

def dataPipeline(pathToDirectory:str, yearsToFetch:list, override:bool):
    for year in yearsToFetch:
        dowload_games_for_season(year, pathToDirectory, override)
    return

"""
The first 4 digits identify the season of the game (ie. 2017 for the 2017-2018 season). The next 2 digits give the type of game,
 where 01 = preseason, 02 = regular season, 03 = playoffs, 04 = all-star. The final 4 digits identify the specific game number. 
 For regular season and preseason games, this ranges from 0001 to the number of games played. 
 (1271 for seasons with 31 teams (2017 and onwards) and 1230 for seasons with 30 teams). 
 For playoff games, the 2nd digit of the specific number gives the round of the playoffs, the 3rd digit specifies the matchup, 
 and the 4th digit specifies the game (out of 7).
"""
def build_game_id(year: int, forRegularSeason: bool) -> int:

    regularSeason = '02'
    regularFirstGame = '0001'

    playoffsSeason = '03'
    playoffsFirstGame = '0111'
    if forRegularSeason :
        #example of a game id -- 2017021001
        return int((f'{year}{regularSeason}{regularFirstGame}'))
    if not forRegularSeason :
        return int((f'{year}{playoffsSeason}{playoffsFirstGame}'))

def checkForDirectoryExistence(pathToDirectory):
    try:
        if path.exists(pathToDirectory) :
            print('Path to directory is valid')
            return True
        else:
            print('Path to directory is not valid, we will create a structure')
            os.makedirs(pathToDirectory)
            print('The directory was created')
    except OSError as error:
        print(error)
        exit(0)

    pass

dataPipeline('./hockey',[2017,2018,2019,2020,2021],False)
