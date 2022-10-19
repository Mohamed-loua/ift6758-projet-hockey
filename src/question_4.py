import pandas as pd
import numpy as np
import os
import os as path
import json
import requests
from src.DataExtractor import DataExtractor


data_extractor = DataExtractor()

data = data_extractor.get_game_data('./notebooks/hockey/Season20172018/Regular20172018/2017020001.json')
data = data_extractor.clean_json(data)
pd_data = data_extractor.create_panda_dataframe(data).reset_index()
print(pd_data)
