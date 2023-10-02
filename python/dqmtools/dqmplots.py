import sys

import rawdatautils.unpack.utils
import dqmtools.dataframe_creator as dfc
from dqmtools.dqmtools import *

try:
    import pandas as pd
    import plotly.express as px
    import plotly.graph_objects as go
    import numpy as np

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

def plot_WIBEth_by_channel(df_dict,var,det_name,run=None,trigger=None,seq=None,yrange=None,jpeg_base=None):
    
    if f"detd_k{det_name}_kWIBEth" not in df_dict.keys():
        print(f"Can not make plots for detd_k{det_name}_kWIBEth, no DATA found")
        return
    
    df_tmp, index = dfc.select_record(df_dict[f"detd_k{det_name}_kWIBEth"],run,trigger,seq)
    df_tmp = df_tmp.reset_index()
    df_tmp["apa_plane_label"] = df_tmp[["apa","plane"]].apply(lambda x: f'{x.apa}, Plane {x.plane}',axis=1)
    
    trigger_time = get_CERN_timestamp(df_dict,index)

    fig = px.scatter(df_tmp,x="channel",y=var,color="apa_plane_label",width=1000,height=600)
    fig.update_layout(xaxis_title='Channel',yaxis_title=var,legend_title='APA/CRP, Plane',
                          title=f'Run {index.run}, Record ({index.trigger,index.sequence}), Time {trigger_time}')
    if yrange is not None:
       fig.update_yaxes(range=yrange)
    if jpeg_base is not None:
        fig.write_image(f"{jpeg_base}_run{index.run}_trigger{index.trigger}_seq{index.sequence}.jpeg")
    return fig

def plot_WIBEth_pulser_by_channel(df_dict,det_name,run=None,trigger=None,seq=None,jpeg_base=None):
    
    if f"detd_k{det_name}_kWIBEth" not in df_dict.keys():
        print(f"Can not make plots for detd_k{det_name}_kWIBEth, no DATA found")
        return
    
    df_tmp, index = dfc.select_record(df_dict[f"detd_k{det_name}_kWIBEth"],run,trigger,seq)
    df_tmp= df_tmp.reset_index()
    
    trigger_time = get_CERN_timestamp(df_dict,index);
    
    fig = px.scatter(df_tmp,x="channel",y=["adc_max","adc_min","adc_median"],
                     width=1000,height=600)
    fig.update_layout(xaxis_title='Channel',
                      yaxis_title="ADC value",
                      legend_title=None,
                      title=f'WIB Pulser Check: Run {index.run}, Record ({index.trigger,index.sequence}), Time {trigger_time}')
    if jpeg_base is not None:
        fig.write_image(f"{jpeg_base}_run{index.run}_trigger{index.trigger}_seq{index.sequence}.jpeg")
    return fig

def plot_DAPHNE_stats(df_dict,det_names=["detd_kHD_PDS_kDAPHNEStream", "detd_kHD_PDS_kDAPHNE" ],run=None,trigger=None,seq=None,jpeg_base=None):
    
    for daphne_type in det_names:

        if daphne_type not in df_dict.keys():
            print(f"Can not make plots for {daphne_type}, no DATA found")
            continue
        
        df_tmp, index = dfc.select_record(df_dict[daphne_type],run,trigger,seq)
        df_tmp= df_tmp.reset_index()

        fig = px.scatter(df_tmp,x="channel",y="adc_mean", error_y="adc_rms",
                     range_y=[np.min(df_tmp["adc_min"]), np.max(df_tmp["adc_max"])], width=1000,height=600)
        trigger_time = get_CERN_timestamp(df_dict,index)

        fig.update_layout(xaxis_title='Channel',
                      yaxis_title="Mean ADC value",
                      legend_title=None,
                      title=f'DAPHNE Streaming mode check: Run {index.run}, Record ({index.trigger,index.sequence}), Time {trigger_time}',
                      xaxis_title_font=dict(size=18), yaxis_title_font=dict(size=18))
    
        if jpeg_base is not None:
            fig.write_image(f"{jpeg_base}_ADCstats_run{index.run}_{daphne_type[13:]}_trigger{index.trigger}_seq{index.sequence}.jpeg")

        
def plot_DAPHNE_baseline(df_dict,det_names=["detd_kHD_PDS_kDAPHNE"],run=None,trigger=None,seq=None,jpeg_base=None):
    
    for daphne_type in det_names:

        if daphne_type not in df_dict.keys():
            print(f"Can not make plots for {daphne_type[13:]}, no DATA found")
            continue
        
        df_tmp, index = dfc.select_record(df_dict[daphne_type],run,trigger,seq)
        df_tmp= df_tmp.reset_index()

        fig = px.scatter(df_tmp,x="baseline",y="trigger_sample_value",  width=1000,height=600)
        trigger_time = get_CERN_timestamp(df_dict,index)

        fig.update_layout(xaxis_title='Baseline',
                      yaxis_title="Trigger sample values",
                      legend_title=None,
                      title=f'DAPHNE baseline check: Run {index.run}, Record ({index.trigger,index.sequence}), Time {trigger_time}',
                      xaxis_title_font=dict(size=18), yaxis_title_font=dict(size=18))
    
        if jpeg_base is not None:
            fig.write_image(f"{jpeg_base}_baseline_run{index.run}_{daphne_type[13:]}_trigger{index.trigger}_seq{index.sequence}.jpeg")

    

    




