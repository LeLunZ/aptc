from pathlib import Path

data_path = Path('../Data')
shapes_path = Path('../shapes')
db_path = Path('../db')
logs_path = Path('../logs')

pickle_path = data_path / 'pickle'

config_path = data_path / 'config.json'
state_path = pickle_path / 'state.pkl'
chrome_driver_path = Path('../chromedriver')
