import json
import os

try:
    config_env = os.environ['CRAWLER_CONFIG']
except:
    config_env = 'default'

with open('./Data/config.json') as config_file:
    config = json.load(config_file)[config_env]


def getConfig(path=None):
    if path is None:
        return config
    paths = path.split('.')
    sub_config = config
    for subPath in paths:
        if subPath not in sub_config:
            raise KeyError(f'Argument {subPath} not in config')
        sub_config = sub_config[subPath]
    return sub_config
