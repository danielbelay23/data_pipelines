import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))

DATA_DIR = os.path.abspath(os.path.join(PROJECT_ROOT, 'data'))
RAW_DATA_DIR = os.path.abspath(os.path.join(DATA_DIR, 'raw'))
PROCESSED_DIR = os.path.abspath(os.path.join(DATA_DIR, 'processed'))

COOKIES_FILE = os.path.abspath(os.path.join(RAW_DATA_DIR, 'cookies.json'))
CONFIG_FILE = os.path.abspath(os.path.join(RAW_DATA_DIR, 'user_config.json'))
FOLLOWING_FILE = os.path.abspath(os.path.join(RAW_DATA_DIR, 'following.json'))
TWEETS_FILE = os.path.abspath(os.path.join(RAW_DATA_DIR, 'tweets.json'))
LOGGING_FILE = os.path.abspath(os.path.join(PROCESSED_DIR, 'logging.json'))
