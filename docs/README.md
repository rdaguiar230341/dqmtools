# dqmtools
Tools for on the spot Data Quality Monitoring. With dqmtools package it is possible to perform a number of basic checks for a given data file. 
Main script:
```
scripts/dqm_analyzer.py 
```
By default script will look for HD TPC data and analyze only first record in a given file. When finished script will print out in a table with possible check results -- INVALID,OK,WARNING,BAD. 

### Usage examples
The most basic usage requires only one argument -- data file and works for the HD TPC data:
```
./dqm_analyzer.py /data1/np04_hd_run022752_0000_dataflow0_datawriter_0_20230925T084543.hdf5.copied
```
In case of VD TPC additional argument `--vd` is required:
```
./dqm_analyzer.py --vd /data1/np02_vd_run022748_0000_dataflow0_datawriter_0_20230925T074747.hdf5
```
Adding `--pds` will initialize checks related to the DAPHNE data and print results in a separate table:
```
./dqm_analyzer.py --pds /data1/np04_hd_run022752_0000_dataflow0_datawriter_0_20230925T084543.hdf5.copied
```
And to add plotting of some stats for the collected data:
./dqm_analyzer.py --make-plots --pds /data1/np04_hd_run022752_0000_dataflow0_datawriter_0_20230925T084543.hdf5.copied
