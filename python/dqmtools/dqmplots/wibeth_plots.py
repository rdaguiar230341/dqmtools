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

from .plot_utils import *

def plot_WIBEth_by_channel(df_dict,var,det_name,run=None,trigger=None,seq=None,yrange=None,jpeg_base=None):

    rename_PD2HD_APAs(df_dict)

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

    rename_PD2HD_APAs(df_dict)

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

    rename_PD2HD_APAs(df_dict)

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

def plot_WIBEth_adc_map(df_dict,tpc_det_key,apa,plane,
                        offset=True,
                        orientation="vertical",colorscale='plasma',color_range=(-256,256),
                        run=None,trigger=None,seq=None):

    rename_PD2HD_APAs(df_dict)

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
    df_tmp = df_tmp.sort_values("channel")

    if orientation=="horizontal":
        xdata = df_tmp.iloc[0]["timestamps_trg_sub"]
        ydata = df_tmp["channel"].values
        zdata = np.vstack(df_tmp["adcs"].values)
        yaxis_title='Offline Channel'
        xaxis_title='DTS time ticks (16ns)'
    else:
        ydata = df_tmp.iloc[0]["timestamps_trg_sub"]
        xdata = df_tmp["channel"].values
        zdata = np.vstack(df_tmp["adcs"].values).T
        xaxis_title='Offline Channel'
        yaxis_title='DTS time ticks (16ns)'

    fig=px.imshow(zdata,
                  aspect="auto", origin='lower',
                  x=list(xdata),y=list(ydata),
                  color_continuous_scale=colorscale,
                  zmin=color_range[0],zmax=color_range[1])

    fig.update_layout(
        yaxis_title=yaxis_title,
        xaxis_title=xaxis_title,
    )

    return fig

def plot_WIBEth_waveform(df_dict,tpc_det_key,channel,
                         offset=False,overlay_tps=False,
                         run=None,trigger=None,seq=None):

    rename_PD2HD_APAs(df_dict)

    if tpc_det_key not in df_dict.keys():
        print(f"Can not make plots for {tpc_det_key}, no DATA found")
        return None
    tpc_wvfm_key = "detw"+tpc_det_key[4:]
    df_tmp = df_dict[tpc_wvfm_key]
    idx_names = df_tmp.index.names
    df_tmp = df_tmp.reset_index()
    df_tmp = df_tmp.loc[df_tmp["channel"]==channel]
    df_tmp = df_tmp.set_index(idx_names)

    df_tmp = df_tmp.merge(df_dict["frh"]["trigger_timestamp_dts"],left_index=True,right_index=True)
    if offset:
        df_tmp = df_tmp.merge(df_dict[tpc_det_key]["adc_mean"],left_index=True,right_index=True)

    df_tmp, index = dfc.select_record(df_tmp,run,trigger,seq)
    df_tmp = df_tmp.reset_index()

    df_tmp["timestamps_trg_sub"] = df_tmp.apply(lambda x: x.timestamps.astype(np.int64) - x.trigger_timestamp_dts,axis=1)
    yaxis_title = "ADC counts"
    if offset:
        df_tmp["adcs"] = df_tmp["adcs"]-df_tmp["adc_mean"]
        yaxis_title = yaxis_title + " (pedestal subtracted)"

    print(df_tmp)
    print(df_tmp["timestamps"].values[0])
    print(df_tmp["adcs"].values[0])
    fig = go.Figure(data=go.Scatter(x=df_tmp["timestamps_trg_sub"].values[0], y=df_tmp["adcs"].values[0]))


    fig.update_layout(xaxis_title='DTS Timestmap (16ns) relative to trigger',
                      yaxis_title=yaxis_title,
                      title=f"Waveform for channel {channel}")

    return fig
