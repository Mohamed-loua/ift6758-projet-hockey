import pandas as pd
import numpy as np
import os
import os as path
import json
import requests

# from ift6758.data.question_1 import json_to_single_file


def get_game(pathToFile):
    file = open(pathToFile, 'r', encoding='utf-8')
    jsonStr = file.read()
    seasonDict = json.loads(jsonStr)
    file.close()
    return seasonDict


def clean_json(jsonDict: dict)-> dict:
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

data = get_game('./notebooks/hockey/Season20172018/Regular20172018/2017020001.json')
data = clean_json(data)

pd_data = pd.DataFrame(data.values()) #Creates a data frame from a dict example

#Prendre data qui contient 5 sub structures et les convertirs en une seule structure pour l'ajouter a un data frame
#Peut faire une methode qui prend un play de data et on fetch les donnees que l'on veut sinon voir comment extrait l'information
#du dictionnaire. On peut faire une methode par structure afin de rendre le tout plus grannulaire.
#columns=['heure','periode','gameid','shootingTeam','receivingTeam','position(x,y)','wasAGoal','shooterName','goalieName','wasEmptyNet', 'wasNumericAdvantage']


print("HI")