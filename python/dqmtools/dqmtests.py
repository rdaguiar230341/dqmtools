from dqmtools.dqmtools import *

import numpy as np
import operator

class CheckAllExpectedFragmentsTest(DQMTest):

    def __init__(self):
        super().__init__()
        self.name = 'CheckAllExpectedFragmentsTest'

    def run_test(self,df_dict):
        df_tmp = (df_dict["trh"]["n_fragments"]!=df_dict["trh"]["n_requested_components"])
        n_not_matched = df_tmp.sum()
        if n_not_matched==0:
            return DQMTestResult(DQMResultEnum.OK,f'OK')
        else:
            return DQMTestResult(DQMResultEnum.BAD,f'{n_not_matched} / {len(df_tmp)} records missing fragments.')

class CheckTimestampDiffs_WIBEth(DQMTest):
    
    def __init__(self,det_name):
        super().__init__()
        self.name = f'CheckTimestampDiffs_WIBEth_{det_name}'
        self.det_head_key=f'deth_k{det_name}_kWIBEth'
        
    def run_test(self,df_dict):

        if self.det_head_key not in df_dict.keys():
            return DQMTestResult(DQMResultEnum.WARNING,f'Could not find {self.det_head_key} in DataFrame dict.')
        
        df_tmp = df_dict[self.det_head_key]
        df_tmp["ts_diff_wrong"] = df_tmp.apply(lambda x: (x.timestamp_dts_diff_vals!=x.sampling_period).sum(), axis=1)
        n_ts_diff_wrong = df_tmp["ts_diff_wrong"].sum()
        if n_ts_diff_wrong==0:
            return DQMTestResult(DQMResultEnum.OK,f'OK')
        else:
            return DQMTestResult(DQMResultEnum.BAD,
                                 f'{n_ts_diff_wrong} / {len(df_tmp)} fragments have bad timestamp differences.')

class CheckWIBEth_COLDDATA_Timestamps_Aligned(DQMTest):

    def __init__(self,det_name):
        super().__init__()
        self.name = f'CheckWIBEth_COLDDATA_Timestamps_Aligned_{det_name}'
        self.det_head_key=f'deth_k{det_name}_kWIBEth'

    def run_test(self,df_dict):

        if self.det_head_key not in df_dict.keys():
            return DQMTestResult(DQMResultEnum.WARNING,f'Could not find {self.det_head_key} in DataFrame dict.')

        df_tmp = df_dict[self.det_head_key]
        df_tmp["timestamp_dts_first_15bit"] = df_tmp["timestamp_dts_first"] & 0x7fff
        df_tmp["ts_diff_cd0_dts"] = df_tmp["timestamp_dts_first_15bit"]-df_tmp["colddata_timestamp_0_first"]
        df_tmp["ts_diff_cd1_dts"] = df_tmp["timestamp_dts_first_15bit"]-df_tmp["colddata_timestamp_1_first"]
        
        df_tmp = df_tmp[ (df_tmp["ts_diff_cd0_dts"]!=0) | (df_tmp["ts_diff_cd1_dts"]!=0) ]

        if len(df_tmp)==0:
            return DQMTestResult(DQMResultEnum.OK,f'OK')
        else:
            return DQMTestResult(DQMResultEnum.BAD,
                                 f'{len(df_tmp)} COLDDATA timestamps across {len(df_tmp)} fragments have bad timestamp differences relative to DTS.')

        
class CheckWIBEth_Header_Value(DQMTest):
    
    def __init__(self,det_name,field,good_value):
        super().__init__()
        self.name = f'CheckWIBEth_Header_Value_{det_name}'
        self.det_head_key=f'deth_k{det_name}_kWIBEth'
        self.header_field = field
        self.good_value = good_value

    def run_test(self,df_dict):

        if self.det_head_key not in df_dict.keys():
            return DQMTestResult(DQMResultEnum.WARNING,f'Could not find {self.det_head_key} in DataFrame dict.')
        
        df_tmp = df_dict[self.det_head_key].rename(columns={f"{self.header_field}_vals":"vals",f"{self.header_field}_idx":"idx"})
        df_tmp["n_empty"] = df_tmp.apply(lambda x: (1 if x.vals == [] else 0), axis=1)
        n_empty_err = df_tmp["n_empty"].sum()
        if n_empty_err != 0:
            print(df_tmp[["vals","idx"]])
            return DQMTestResult(DQMResultEnum.BAD,
                                 f'{n_empty_err} {self.header_field}!={self.good_value} errors across {len(df_tmp)} fragments.')
        
        df_tmp["n_err"] = df_tmp.apply(lambda x: (x.vals!=self.good_value).sum(), axis=1)
        n_total_err = df_tmp["n_err"].sum()
        if n_total_err==0:
            return DQMTestResult(DQMResultEnum.OK,f'OK')
        else:
            print(df_tmp[["vals","idx"]])
            return DQMTestResult(DQMResultEnum.BAD,
                                 f'{n_total_err} {self.header_field}!={self.good_value} errors across {len(df_tmp)} fragments.')

        
class CheckWIBEth_COLDDATA_Timestamp_0_Diff(CheckWIBEth_Header_Value):
    
    def __init__(self,det_name):
        super().__init__(det_name,"colddata_timestamp_0_diff",512/16*64)
        self.name = f'CheckWIBEth_COLDDATA_Timestamp_0_Diff_{det_name}'
        
class CheckWIBEth_COLDDATA_Timestamp_1_Diff(CheckWIBEth_Header_Value):
    
    def __init__(self,det_name):
        super().__init__(det_name,"colddata_timestamp_1_diff",512/16*64)
        self.name = f'CheckWIBEth_COLDDATA_Timestamp_1_Diff_{det_name}'
        
class CheckWIBEth_CRC_Err(CheckWIBEth_Header_Value):
    
    def __init__(self,det_name):
        super().__init__(det_name,"crc_err",0)
        self.name = f'CheckWIBEth_CRC_Err_{det_name}'
        
class CheckWIBEth_Pulser(CheckWIBEth_Header_Value):
    
    def __init__(self,det_name):
        super().__init__(det_name,"pulser",0)
        self.name = f'CheckWIBEth_Pulser_{det_name}'

class CheckWIBEth_Calibration(CheckWIBEth_Header_Value):
    
    def __init__(self,det_name):
        super().__init__(det_name,"calibration",0)
        self.name = f'CheckWIBEth_Calibration_{det_name}'

class CheckWIBEth_Ready(CheckWIBEth_Header_Value):
    
    def __init__(self,det_name):
        super().__init__(det_name,"ready",0)
        self.name = f'CheckWIBEth_Ready_{det_name}'

class CheckWIBEth_Context(CheckWIBEth_Header_Value):
    
    def __init__(self,det_name):
        super().__init__(det_name,"context",0)
        self.name = f'CheckWIBEth_Context_{det_name}'
        
class CheckWIBEth_CD(CheckWIBEth_Header_Value):
    
    def __init__(self,det_name):
        super().__init__(det_name,"cd",0)
        self.name = f'CheckWIBEth_CD_{det_name}'

class CheckWIBEth_Link_Valid(CheckWIBEth_Header_Value):
    
    def __init__(self,det_name):
        super().__init__(det_name,"link_valid",3)
        self.name = f'CheckWIBEth_Link_Valid_{det_name}'

class CheckWIBEth_LOL(CheckWIBEth_Header_Value):
    
    def __init__(self,det_name):
        super().__init__(det_name,"lol",0)
        self.name = f'CheckWIBEth_LOL_{det_name}'

class CheckWIBEth_WIB_Sync(CheckWIBEth_Header_Value):
    
    def __init__(self,det_name):
        super().__init__(det_name,"wib_sync",0)
        self.name = f'CheckWIBEth_WIB_Sync_{det_name}'

class CheckWIBEth_FEMB_Sync(CheckWIBEth_Header_Value):
    
    def __init__(self,det_name):
        super().__init__(det_name,"femb_sync",3)
        self.name = f'CheckWIBEth_FEMB_Sync_{det_name}'

        
class CheckTimestampsAligned(DQMTest):

    def __init__(self,det_id,verbose=True):
        super().__init__()
        self.det_id = det_id
        self.name = f'CheckTimestampsAligned_{det_id}'
        self.verbose = verbose
        
    def any_different(arr):
        return (arr.values!=arr.values[0]).sum()

    def unique(arr):
        return np.unique(arr.values,return_counts=True)
    
    def run_test(self,df_dict):
        df_tmp = df_dict["daqh"].loc[df_dict["daqh"]["det_id"]==self.det_id]

        if len(df_tmp)==0:
            return DQMTestResult(DQMResultEnum.WARNING,f'WARNING: No components found with detector id {self.det_id}.')
        
        df_tmp_gb = df_tmp.groupby(by=["run","trigger","sequence"])["timestamp_first_dts"].agg(CheckTimestampsAligned.unique)
        df_tmp_gb_n = df_tmp_gb.apply(lambda x: len(x[1]))
        n_different = (df_tmp_gb_n!=1).sum()

        if n_different==0:
            return DQMTestResult(DQMResultEnum.OK,f'OK')
        else:
            if self.verbose:
                df_tmp_gb_mode = df_tmp_gb.apply(lambda x: x[0][np.argmax(x[1])])
                df_tmp = df_tmp.join(df_tmp_gb_mode,rsuffix='_majority')
                df_tmp = df_tmp.loc[(df_tmp["timestamp_first_dts"]!=df_tmp["timestamp_first_dts_majority"])]

                df_tmp["timestamp_diff"] = df_tmp["timestamp_first_dts"]-df_tmp["timestamp_first_dts_majority"]
                n_different = len(np.unique(df_tmp.reset_index()["src_id"].apply(lambda x: int(x))))
                print("\nFRAGMENTS FAILING TIMESTAMP ALIGNMENT")
                print(tabulate(df_tmp.reset_index()[["trigger","sequence","crate_id","slot_id","stream_id","timestamp_first_dts","timestamp_first_dts_majority","timestamp_diff"]],
                                headers=["Record","Seq.","Crate","Slot","Stream","Timestamp (first)","Majority timestamp","Difference"],
                                showindex=False,tablefmt='pretty'))

            return DQMTestResult(DQMResultEnum.BAD,
                                 f'{n_different} sources have some timestamp misalignment for det_id {self.det_id}.')

class CheckNFrames_WIBEth(DQMTest):

    def __init__(self):
        super().__init__()
        self.name = "CheckNFrames_WIBEth"

    def run_test(self,df_dict):
        df_tmp = df_dict["frh"].loc[df_dict["frh"]["fragment_type"]==12][["window_begin_dts","window_end_dts"]]
        if len(df_tmp)==0:
            return DQMTestResult(DQMResultEnum.WARNING,f'WARNING: No WIBEth components found.')
        df_tmp["expected_frames"] = np.floor((df_tmp["window_end_dts"]-df_tmp["window_begin_dts"])/(32*64))+1
        df_tmp = df_tmp.join(df_dict["daqh"][["n_obj"]])
        n_frames_wrong = (df_tmp["expected_frames"]!=df_tmp["n_obj"]).sum()
        if n_frames_wrong==0:
            return DQMTestResult(DQMResultEnum.OK,f'OK')
        else:            
            return DQMTestResult(DQMResultEnum.BAD,
                                 f'{n_frames_wrong} / {len(df_tmp)} WIBEth fragments have the wrong number of frames.')


class CheckRMS_WIBEth(DQMTest):

    def __init__(self,det_name,threshold=100,operator=operator.gt,verbose=False):
        super().__init__()
        self.name = 'CheckRMS_{det_name}'
        self.det_data_key=f'detd_k{det_name}_kWIBEth'

        if not isinstance(threshold,list): #one value for all planes
            self.df_threshold = pd.DataFrame({"plane":[0,1,2],"threshold":np.full(3,threshold)})
        elif len(threshold)==1: #one value for all planes
            self.df_threshold = pd.DataFrame({"plane":[0,1,2],"threshold":np.full(3,threshold[0])})
        elif len(threshold)==2: #two values, first induction, second collection
            self.df_threshold = pd.DataFrame({"plane":[0,1,2],"threshold":np.array([threshold[0],threshold[0],threshold[1]])})
        elif len(threshold)==3: #three values, one for each plane
            self.df_threshold = pd.DataFrame({"plane":[0,1,2],"threshold":np.array(threshold)})
        else:
            print(f'Threshold length {len(threshold)} is not valid.',threshold)
            raise ValueError
        self.operator = operator
        self.verbose = verbose
        

    def run_test(self,df_dict):

        if self.det_data_key not in df_dict.keys():
            return DQMTestResult(DQMResultEnum.WARNING,f'Could not find {self.det_data_key} in DataFrame dict.')
        
        df_tmp = df_dict[self.det_data_key].reset_index().merge(self.df_threshold,on=["plane"])
        df_tmp = df_tmp[["channel","adc_rms","threshold"]].groupby(by="channel").mean().reset_index()
        df_tmp = df_tmp.loc[self.operator(df_tmp["adc_rms"],df_tmp["threshold"])]
        n_rms_bad = len(np.unique(df_tmp["channel"]))

        if n_rms_bad==0:
            return DQMTestResult(DQMResultEnum.OK,f'OK')

        else:
            if self.verbose:
                print("CHANNELS FAILING RMS CHECK")
                df_tmp = df_tmp.merge(df_dict[self.det_data_key].reset_index()[["channel","apa","plane"]].drop_duplicates(["channel"]),on=["channel"])
                print(tabulate(df_tmp.reset_index()[["channel","adc_rms","apa","plane","threshold"]],
                               headers=["Channel","RMS","APA/CRP","Plane","Threshold"],
                               showindex=False,tablefmt='pretty',floatfmt=".2f"))
            return DQMTestResult(DQMResultEnum.BAD,
                                 f'{n_rms_bad} channels have RMS outside of range.')

class CheckPedestal_WIBEth(DQMTest):

    def __init__(self,det_name,lower_bound=[7500,200],upper_bound=[9500,2000],verbose=False):
        super().__init__()
        self.name = f'CheckPedestal_{det_name}'
        self.det_data_key=f'detd_k{det_name}_kWIBEth'

        if not isinstance(lower_bound,list): #one value for all planes
            self.df_lower_bound = pd.DataFrame({"plane":[0,1,2],"lower_bound":np.full(3,lower_bound)})
        elif len(lower_bound)==1: #one value for all planes
            self.df_lower_bound = pd.DataFrame({"plane":[0,1,2],"lower_bound":np.full(3,lower_bound[0])})
        elif len(lower_bound)==2: #two values, first induction, second collection
            self.df_lower_bound = pd.DataFrame({"plane":[0,1,2],"lower_bound":np.array([lower_bound[0],lower_bound[0],lower_bound[1]])})
        elif len(lower_bound)==3: #three values, one for each plane
            self.df_lower_bound = pd.DataFrame({"plane":[0,1,2],"lower_bound":np.array(lower_bound)})
        else:
            print(f'Lower_Bound length {len(lower_bound)} is not valid.',lower_bound)
            raise ValueError

        if not isinstance(upper_bound,list): #one value for all planes
            self.df_upper_bound = pd.DataFrame({"plane":[0,1,2],"upper_bound":np.full(3,upper_bound)})
        elif len(upper_bound)==1: #one value for all planes
            self.df_upper_bound = pd.DataFrame({"plane":[0,1,2],"upper_bound":np.full(3,upper_bound[0])})
        elif len(upper_bound)==2: #two values, first induction, second collection
            self.df_upper_bound = pd.DataFrame({"plane":[0,1,2],"upper_bound":np.array([upper_bound[0],upper_bound[0],upper_bound[1]])})
        elif len(upper_bound)==3: #three values, one for each plane
            self.df_upper_bound = pd.DataFrame({"plane":[0,1,2],"upper_bound":np.array(upper_bound)})
        else:
            print(f'Upper_Bound length {len(upper_bound)} is not valid.',upper_bound)
            raise ValueError

        self.verbose = verbose
        

    def run_test(self,df_dict):

        if self.det_data_key not in df_dict.keys():
            return DQMTestResult(DQMResultEnum.WARNING,f'Could not find {self.det_data_key} in DataFrame dict.')
        
        df_tmp = df_dict[self.det_data_key].reset_index().merge(self.df_lower_bound,on=["plane"]).merge(self.df_upper_bound,on=["plane"])
        df_tmp = df_tmp[["channel","adc_mean","lower_bound","upper_bound"]].groupby(by="channel").mean().reset_index()
        df_tmp = df_tmp.loc[(df_tmp["adc_mean"]<df_tmp["lower_bound"])|(df_tmp["adc_mean"]>df_tmp["upper_bound"])]
        n_bad = len(np.unique(df_tmp["channel"]))
        if n_bad==0:
            return DQMTestResult(DQMResultEnum.OK,f'OK')
        else:
            if self.verbose:
                print("CHANNELS FAILING PEDESTAL CHECK")
                df_tmp = df_tmp.merge(df_dict[self.det_data_key].reset_index()[["channel","apa","plane"]].drop_duplicates(["channel"]),on=["channel"])
                print(tabulate(df_tmp.reset_index()[["channel","adc_mean","apa","plane","lower_bound","upper_bound"]],
                               headers=["Channel","Pedestal","APA/CRP","Plane","Lower Bound","Upper Bound"],
                               showindex=False,tablefmt='pretty',floatfmt=".2f"))
            return DQMTestResult(DQMResultEnum.BAD,
                                 f'{n_bad} channels have pedestal outside of range.')

class CheckEmptyFragments_DAPHNE(DQMTest):

    def __init__(self):
        super().__init__()
        self.name = "CheckEmptyFragments_DAPHNE"

    def run_test(self,df_dict):
        df_tmp1 = df_dict["frh"].loc[df_dict["frh"]["fragment_type"]==3]

        if len(df_tmp1)==0:
            return DQMTestResult(DQMResultEnum.WARNING,f"WARNING: No Self-triggered DAPHNE data found.")
        
        n_emptyFrames          = len(df_tmp1.loc[df_tmp1["data_size_bytes"] == 0])
        n_filledFrames         = len(df_tmp1.loc[df_tmp1["data_size_bytes"] != 0])

        if n_emptyFrames != 0:
            return DQMTestResult(DQMResultEnum.BAD, f'{n_emptyFrames} fragments are empty ({n_filledFrames} are fine).')
        else:
            return DQMTestResult(DQMResultEnum.OK,f'OK')

class CheckFramesInTimeWindow_DAPHNE(DQMTest):

    def __init__(self):
        super().__init__()
        self.name = "CheckFramesInTimeWindow_DAPHNE"

    def run_test(self,df_dict):
        ...

class CheckTimestampDiffs_DAPHNE(DQMTest):
    def __init__(self):
        super().__init__()
        self.name = "CheckTimestampDiffs_DAPHNE"

    def run_test(self, df_dict, verbose=False):

        n_bad_stream, n_bad = 0, 0

        if "detd_kHD_PDS_kDAPHNEStream" not in df_dict.keys() and "detd_kHD_PDS_kDAPHNE" not in df_dict.keys():
            return DQMTestResult(DQMResultEnum.WARNING,f'WARNING: No data for DAPHNE found.')
        
        if "deth_kHD_PDS_kDAPHNEStream" in df_dict.keys():

            tmp_df_stream  = df_dict["deth_kHD_PDS_kDAPHNEStream"]
            tmp_df_stream["ts_check"] = tmp_df_stream.apply(lambda x: 1 if (len(x.ts_diffs_vals)!=1) else 0, axis=1)
            n_bad_stream = tmp_df_stream["ts_check"].sum()

        if "deth_kHD_PDS_kDAPHNE" in df_dict.keys():

            tmp_df  = df_dict["deth_kHD_PDS_kDAPHNEStream"]
            tmp_df["ts_check"] = tmp_df.apply(lambda x: 1 if (len(x.ts_diffs_vals)!=1) else 0, axis=1)
            n_bad = tmp_df["ts_check"].sum()

        if n_bad == 0 and n_bad_stream == 0:
            return DQMTestResult(DQMResultEnum.OK,f'OK')
        else:
            if verbose:
                if n_bad != 0:
                    print(tabulate(tmp_df.reset_index()[["trigger","sequence","ts_diffs_vals", "ts_diffs_counts", "ts_check"]],
                                   headers=["record","sequence","ts_diffs","ts_diffs_counts","Check"],
                                   showindex=False,tablefmt='pretty',floatfmt=".2f"))
                elif n_bad_stream != 0:
                    print(tabulate(tmp_df_stream.reset_index()[["trigger","sequence","ts_diffs_vals", "ts_diffs_counts", "ts_check"]],
                                   headers=["record","sequence","ts_diffs","ts_diffs_counts","Check"],
                                   showindex=False,tablefmt='pretty',floatfmt=".2f"))

            return DQMTestResult(DQMResultEnum.BAD, f'{n_bad+n_bad_stream} links fail TS difference check.')


class CheckADCData_DAPHNE(DQMTest):

    def __init__(self):
        super().__init__()
        self.name = "CheckADCData_DAPHNE"
    
    def run_test(self, df_dict):

        if "detd_kHD_PDS_kDAPHNEStream" not in df_dict.keys() and "detd_kHD_PDS_kDAPHNE" not in df_dict.keys():
            return DQMTestResult(DQMResultEnum.WARNING,f'WARNING: No data for DAPHNE found.')
        
        elif "detd_kHD_PDS_kDAPHNEStream" in df_dict.keys():

            tmp_df_stream  = df_dict["detd_kHD_PDS_kDAPHNEStream"]
            means = np.array(df_dict["detd_kHD_PDS_kDAPHNEStream"]["adc_mean"])
            rmss  = np.array(df_dict["detd_kHD_PDS_kDAPHNEStream"]["adc_rms"])

            if np.any(means) == 0 or np.any(rmss) == 0:

                n_bad_means_stream = len(means[np.where(means == 0)])
                n_bad_rmss_stream  = len(rmss[np.where(rmss == 0)])
                return DQMTestResult(DQMResultEnum.BAD, f'{np.max(n_bad_means_stream, n_bad_rmss_stream)} channels have problems')
            
            else:
                return DQMTestResult(DQMResultEnum.OK,f'OK')
            
        else:

            tmp_df_stream  = df_dict["detd_kHD_PDS_kDAPHNE"]
            means = np.array(df_dict["detd_kHD_PDS_kDAPHNE"]["adc_mean"])
            rmss  = np.array(df_dict["detd_kHD_PDS_kDAPHNE"]["adc_rms"])

            if np.any(means) == 0 or np.any(rmss) == 0:

                n_bad_means = len(means[np.where(means == 0)])
                n_bad_rmss  = len(rmss[np.where(rmss == 0)])
                return DQMTestResult(DQMResultEnum.BAD, f'{np.max(n_bad_means, n_bad_rmss)} channels have problems')
            
            else:
                return DQMTestResult(DQMResultEnum.OK,f'OK')


        

