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

from .plot_utils import get_CERN_timestamp

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

def plot_WIBETH_by_channel_DQM(df_dict,var,tpc_det_key,run=None,trigger=None,seq=None):

    if tpc_det_key not in df_dict.keys():
        print(f"Can not make plots for {tpc_det_key}, no DATA found")
        return None

    df_tmp, index = dfc.select_record(df_dict[tpc_det_key],run,trigger,seq)

    df_tmp = df_tmp.reset_index()
    df_tmp = df_tmp[["channel",var,"plane","apa"]]

    fig_mean = px.scatter(df_tmp,x="channel",y=var,color="apa",facet_col="plane")
    fig_mean.for_each_annotation(lambda a: a.update(text=f"Plane {a.text.split('=')[-1]}"))

    return fig_mean


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

def plot_adc_map(df_dict,tpc_det_key,apa,plane,
                 offset=True,
                 orientation="vertical",colorscale='plasma',color_range=(-256,256),
                 run=None,trigger=None,seq=None):

    if tpc_det_key not in df_dict.keys():
        print(f"Can not make plots for {tpc_det_key}, no DATA found")
        return None
    tpc_wvfm_key = "detw"+tpc_det_key[4:]

    df_tmp = df_dict[tpc_wvfm_key]
    df_tmp = df_tmp.loc[(df_tmp["apa"]==apa)&(df_tmp["plane"]==plane)]

    df_tmp = df_tmp.merge(df_dict["frh"]["trigger_timestamp_dts"],left_index=True,right_index=True)
    if offset:
        df_tmp = df_tmp.merge(df_dict[tpc_det_key]["adc_mean"],left_index=True,right_index=True)

    df_tmp, index = dfc.select_record(df_tmp,run,trigger,seq)
    df_tmp = df_tmp.reset_index()
    df_tmp["timestamps_trg_sub"] = df_tmp.apply(lambda x: x.timestamps.astype(np.int64) - x.trigger_timestamp_dts,axis=1)
    if offset:
        df_tmp["adcs"] = df_tmp["adcs"]-df_tmp["adc_mean"]

    df_tmp.sort_values("channel")
    print(df_tmp["channel"],df_tmp.iloc[0]["timestamps_trg_sub"])


    if orientation=="horizontal":
        xdata = df_tmp.iloc[0]["timestamps_trg_sub"]
        ydata = df_tmp["channel"]
        zdata = df_tmp["adcs"].T
    else:
        ydata = df_tmp.iloc[0]["timestamps_trg_sub"]
        xdata = df_tmp["channel"]
        zdata = df_tmp["adcs"]

    print(xdata,ydata,zdata)

    fig = go.Figure(data=go.Heatmap(z=df_tmp["adcs"], x=df_tmp["channel"], y=df_tmp.iloc[0]["timestamps_trg_sub"],
                                    zmin=color_range[0], zmax=color_range[1], colorscale=colorscale))

    return fig
