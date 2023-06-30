from os.path import dirname, join
from foursight_core.app_utils import AppUtilsCore

FOURSIGHT_PREFIX = 'foursight-smaht'
DEV_ENV = 'staging'
HOST = ''  # should not be used
CHECK_SETUP_FILE = join(dirname(__file__), AppUtilsCore.CHECK_SETUP_FILE_NAME)
