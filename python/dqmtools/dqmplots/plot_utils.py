import rawdatautils.unpack.utils
import dqmtools.dataframe_creator as dfc
from dqmtools.dqmtools import *

try:
    import pandas as pd
except ModuleNotFoundError as err:
    print(err)
    print("\n\n")
    print("Missing module is likely not part of standard dunedaq releases.")
    print("\n")
    print("Please install the missing module and try again.")
    sys.exit(1)
except:
    raise

def get_CERN_timestamp(df_dict,index):
    trigger_time = df_dict['trh'].loc[(index.run,index.trigger,index.sequence)]["trigger_time"]
    return trigger_time.astimezone(pytz.timezone("Europe/Zurich"))
