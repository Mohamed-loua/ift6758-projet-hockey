import pandas as pd
import numpy as np
import os
import os as path
import json
import requests
from src.DataExtractor import DataExtractor

data_extractor = DataExtractor()

#data = data_extractor.get_game_data('./../ift6758/data/hockey/test/Season20172018/season20172018.json')
#df = data_extractor.get_season_into_dataframe('./../ift6758/data/hockey/test/Season20172018/season20172018.json')
data = data_extractor.get_game_data('./../ift6758/data/hockey/test/Season20172018/Playoff20172018/2017030115.json')
data = data_extractor.clean_json(data)
pd_data = data_extractor.create_panda_dataframe(data)
list_distance = list()
list_x = pd_data['coordinates.x']
list_y = pd_data['coordinates.y']
for i in range(len(list_x)):
    list_distance.append(data_extractor.get_shot_distance_from_goal(list_x[i], list_y[i]))
pd_data['distance_shot'] = list_distance
print('hi')

