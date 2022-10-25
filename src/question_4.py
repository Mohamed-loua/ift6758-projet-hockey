import pandas as pd
import numpy as np
import os
import os as path
import json
import requests
from src.DataExtractor import DataExtractor


data_extractor = DataExtractor()

data = data_extractor.get_game_data('../notebooks/hockey/Season20172018/Playoff20172018/2017030115.json')
game_pk, data = data_extractor.clean_single_game_json(data)
pd_data = data_extractor.create_panda_dataframe_for_one_game(game_pk, data)
print(pd_data.head())
