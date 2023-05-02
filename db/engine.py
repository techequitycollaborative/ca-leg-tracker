from dotenv import load_dotenv
from sqlalchemy import create_engine

import os

_cur_dir = os.path.dirname(os.path.realpath(__file__))
_env_path=os.path.join(_cur_dir, '.env')
load_dotenv(_env_path)
_pw = os.getenv('DATABASE_PW')

engine = create_engine('postgresql+psycopg2://legtracker:' + _pw +
                       '@db-postgresql-sfo3-31638-do-user-10710390-0.b.db.ondigitalocean.com:25060/legtracker')
