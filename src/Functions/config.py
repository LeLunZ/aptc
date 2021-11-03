import json
import os

from constants import config_path

try:
    config_env = os.environ['CRAWLER_CONFIG']
except KeyError:
    config_env = 'default'

with open(config_path) as config_file:
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
