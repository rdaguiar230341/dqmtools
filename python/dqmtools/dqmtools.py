import sys
from enum import Enum
from datetime import datetime
import pytz
import numpy as np

#non-standard imports
try:
    import pandas as pd
    from tabulate import tabulate
except ModuleNotFoundError as err:
    print(err)
    print("\n\n")
    print("Missing module is likely not part of standard dunedaq releases.")
    print("\n")
    print("Please install the missing module and try again.")
    sys.exit(1)
except:
    raise

class DQMResultEnum(Enum):
    INVALID = -1
    BAD = 0
    OK = 1
    WARNING = 2


class DQMTestResult:

    def __init__(self,result=DQMResultEnum.INVALID,message=""):
        self.result = DQMResultEnum(result)
        self.message = str(message)

    def __bool__(self):
        return self.result!=DQMResultEnum.INVALID and self.result!=DQMResultEnum.BAD

    def __str__(self):
        return f'{self.result}: {self.message}'

class DQMTest:
    
    
    def __init__(self,name=None):
        if name is None:
            self.name = 'DQMTest'
        else:
            self.name = name
        self.result = DQMTestResult()
        self.tests = None

    def get_name(self):
        return self.name

    def is_test_suite(self):
        return (self.tests is not None)
    
    def run_test(*args,**kwargs):
        return DQMTestResult()
    
    def do_test(self,*args,**kwargs):
        res = DQMTestResult()
        
        try:
            res = self.run_test(*args,**kwargs)
        except Exception as err:
            res = DQMTestResult(DQMResultEnum.BAD,f'Check raised exception: {err}')

        return { "result": res.result, "message": res.message, "last_update": datetime.now()}


class DQMTestSuite(DQMTest):

    def __init__(self,name):
        super().__init__(name=name)
        self.df_results = pd.DataFrame(columns=["name","result","message","last_update"])
        self.tests = {}

    def register_test(self,test,name=None):
        if name is None:
            name = test.get_name()
        if name in self.tests.keys():
            print(f'Already have a test with name {name}.')
            raise ValueError
        self.tests[name] = test

    def get_test(self,name):
        return self.tests[name]

    def get_all_tests(self):
        return self.tests.values()
        
    def get_all_results(self):
        return self.df_results
        
    def get_latest_results(self):
        return self.df_results.sort_values('last_update',ascending=False).drop_duplicates(["name"])

    def run_test(self,*args,**kwargs):
        new_df = pd.DataFrame([ test.do_test(*args,**kwargs) | {"name": name} for name, test in self.tests.items() ])

        if(len(self.df_results)==0):
            self.df_results = new_df
        else:
            self.df_results = pd.concat([self.df_results,new_df],ignore_index=True)

        if len(self.df_results)==0:
            return DQMTestResult(DQMResultEnum.INVALID,"No registered tests or no valid test results.")

        #If anything bad or invalid, suite is bad
        bad_or_invalid_count = np.count_nonzero(self.df_results['result']==DQMResultEnum.BAD)+np.count_nonzero(self.df_results['result']==DQMResultEnum.INVALID)
        if bad_or_invalid_count>0:
            return DQMTestResult(DQMResultEnum.BAD,f"{bad_or_invalid_count}/{len(self.df_results)} bad or invalid results.")

        #else, if anything in warning, suite is in warning
        warning_count = np.count_nonzero(self.df_results['result']==DQMResultEnum.WARNING)
        if warning_count>0:
            return DQMTestResult(DQMResultEnum.WARNING,f"{warning_count}/{len(self.df_results)} results have warning.")
        
        #else, we're ok
        return DQMTestResult(DQMResultEnum.OK,f"All {len(self.df_results)} results OK.")
        

    def clear_all_results(self):
        self.df_results = pd.DataFrame(columns=["name","result","message","last_update"])

    def clear_old_results(self):
        self.df_results = self.df_results.sort_values('last_update',ascending=False).drop_duplicates(["name"])

    def get_table(self,latest=True,names=[],show_last_update=True,tablefmt='pretty'):
        df_tmp = self.get_latest_results() if latest else self.get_all_results()
        for name in names:
            df_tmp = df_tmp.loc[df_tmp["name"]==name]

        if show_last_update:
            columns=["name","result","message","last_update"]
            headers=['Test Name','Result','Message','Last Update Time']
        else:
            columns=["name","result","message"]
            headers=['Test Name','Result','Message']
            
        return tabulate(df_tmp[columns],
                        headers=headers,
                        tablefmt=tablefmt,showindex=False)

