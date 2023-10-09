import sys

import rawdatautils.unpack.utils
import dqmtools.dataframe_creator as dfc
from dqmtools.dqmtools import *

try:
    import pandas as pd
    import plotly.express as px
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
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
        df_tmp["src_id"] = df_tmp["src_id"].astype(str)

        if daphne_type == "detd_kHD_PDS_kDAPHNE":
            fig = px.scatter(df_tmp, x=df_tmp.index, y="adc_mean", error_y="adc_rms", color="src_id",
                            color_discrete_sequence=px.colors.qualitative.G10,  
                            range_y=[np.min(df_tmp["adc_min"]), np.max(df_tmp["adc_max"])], width=1000,height=600)
            xlabel = "Frame number"
        else:
            fig = px.scatter(df_tmp, x="channel", y="adc_mean", error_y="adc_rms", color="src_id",
                            color_discrete_sequence=px.colors.qualitative.G10, 
                            width=1000,height=600)
            xlabel = "Channel"
            # range_y=[np.min(df_tmp["adc_min"]), np.max(df_tmp["adc_max"])], 

        fig.update_traces(marker_size=12)
        trigger_time = get_CERN_timestamp(df_dict,index)

        fig.update_layout(xaxis_title=xlabel,
                      yaxis_title="Mean ADC value (errors=rms)",
                      legend_title=None,
                      title=f'DAPHNE Streaming mode check: Run {index.run}, Record ({index.trigger,index.sequence}), Time {trigger_time}',
                      xaxis_title_font=dict(size=18), yaxis_title_font=dict(size=18))
    
        if jpeg_base is not None:
            fig.write_image(f"{jpeg_base}_ADCstats_{daphne_type[13:]}_run{index.run}_{daphne_type[13:]}_trigger{index.trigger}_seq{index.sequence}.jpeg")

        
def plot_DAPHNE_baseline(df_dict,det_names=["detd_kHD_PDS_kDAPHNE"],run=None,trigger=None,seq=None,jpeg_base=None):
    
    for daphne_type in det_names:

        if daphne_type not in df_dict.keys():
            print(f"Can not make plots for {daphne_type[13:]}, no DATA found")
            continue
        
        df_tmp, index = dfc.select_record(df_dict[daphne_type],run,trigger,seq)
        df_tmp= df_tmp.reset_index()
        df_tmp["base_diff"] = df_tmp["baseline"] - df_tmp["trigger_sample_value"]
        df_tmp["timediffs"] = df_tmp["timestamp_dts"] - df_tmp["timestamp_dts"][0]

        trace1 = go.Scatter(mode="lines+markers",
                            x=df_tmp["timediffs"],
                            y=df_tmp["base_diff"],
                            name="TSV difference",
                            marker=dict(size=12))
        
        trace2 = go.Scatter(mode='lines',
                            x=df_tmp["timediffs"],
                            y=df_tmp["threshold"],
                            name='Threshold')

        fig = make_subplots()
        fig.add_trace(trace1)
        fig.add_trace(trace2)

        trigger_time = get_CERN_timestamp(df_dict,index)

        fig.update_layout(xaxis_title='Timestamp difference',
                      yaxis_title="ADC counts",
                      legend_title=None,
                      title=f'DAPHNE baseline check: Run {index.run}, Record ({index.trigger,index.sequence}), Time {trigger_time}',
                      xaxis_title_font=dict(size=22), yaxis_title_font=dict(size=22), width=1000, height=600)
    
        if jpeg_base is not None:
            fig.write_image(f"{jpeg_base}_baseline_run{index.run}_{daphne_type[13:]}_trigger{index.trigger}_seq{index.sequence}.jpeg")

    

    




