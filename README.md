# global-lustre-jobstats
glljobstat.py is based on [lljobstat](https://review.whamcloud.com/c/fs/lustre-release/+/48888) with some enhencements!

## Enhancements:
* Aggreate stats over multiple OSS/MDS via SSH in parallel (key and password auth supported)
* Calculate the rate of each job between queries
* Show sum of ops over all jobs
* Show job ops in percentage to total ops
* Keep track of highest ever ops in pickle file
* Process returned strings to yaml like objects in parallel (3x faster)
* Use "naive" parsing to get another 3x speed up over ymal CLoader
* Filter for certain job_ids
* Filter out certain job_ids
* Config file for SSH, OSS/MDS, filter and other settings
* Configure job_id name leght for pretty printing

## Examples
### Help
```
(lljobstat) [root@mXossq1 bolausson]# ./glljobstat.py --help
usage: glljobstat.py [-h] [-c COUNT] [-i INTERVAL] [-n REPEATS]
                     [--param PARAM] [-o] [-m] [-s SERVERS] [--fullname]
                     [--no-fullname] [-f FILTER] [-fm] [-l JOBID_LENGTH] [-t]
                     [-tr] [-trf TOTALRATEFILE] [-p] [-ht] [-nps NUM_PROC_SSH]
                     [-npp NUM_PROC_DATA] [-ncs NUM_CHUNK_SSH]
                     [-ncp NUM_CHUNK_DATA] [-v] [-d | -r]

List top jobs.

optional arguments:
  -h, --help            show this help message and exit
  -c COUNT, --count COUNT
                        the number of top jobs to be listed (default 5).
  -i INTERVAL, --interval INTERVAL
                        the interval in seconds to check job stats again
                        (default 10).
  -n REPEATS, --repeats REPEATS
                        the times to repeat the parsing (default unlimited).
  --param PARAM         the param path to be checked (default *.*.job_stats).
  -o, --ost             check only OST job stats.
  -m, --mdt             check only MDT job stats.
  -s SERVERS, --servers SERVERS
                        Comma separated list of OSS/MDS to query
  --fullname            show full operation name (default False).
  --no-fullname         show abbreviated operations name.
  -f FILTER, --filter FILTER
                        Comma separated list of job_ids to ignore
  -fm, --fmod           Modify the filter to only show job_ids that match the
                        filter instead of removing them
  -l JOBID_LENGTH, --length JOBID_LENGTH
                        Set job_id filename lenght for pretty printing
  -t, --total           Show sum over all jobs for each operation
  -tr, --totalrate      Whenever -tr is is used, a persistent file will be
                        created and keep track of the highest rate ever
  -trf TOTALRATEFILE, --totalratefile TOTALRATEFILE
                        Path to a pickle file which will keep track of the
                        higest rate (default /root/.glljobstatdb.pickle)
  -p, --percent         Show top jobs in percentage to total ops
  -ht, --humantime      Show human readable time instead of timestamp
  -nps NUM_PROC_SSH, --num_proc_ssh NUM_PROC_SSH
                        Number of parallel SSH connections (default cpu count:
                        20).
  -npp NUM_PROC_DATA, --num_proc_data NUM_PROC_DATA
                        Number of parallel data parsing tasks (default cpu
                        count: 20).
  -ncs NUM_CHUNK_SSH, --num_chunk_ssh NUM_CHUNK_SSH
                        Chops the number of parallel SSH jobs into a number of
                        chunks which it submits to the process pool as
                        separate tasks (default: 1)
  -ncp NUM_CHUNK_DATA, --num_chunk_data NUM_CHUNK_DATA
                        Chops the number of parallel data pasing tasks into a
                        number of chunks which it submits to the process pool
                        as separate tasks (default: 1)
  -v, --verbose         Show some debug and timing information

Mutually exclusive options:
  -d, --dif             Show change in counters between two queries
  -r, --rate            Calculate the rate between two queries
```
### Run once, show top 3 jobs:
```
# ./glljobstat.py -n 1 -c 3
---
timestamp: 1693989029
servers_queried: 8
osts_queried: 24
mdts_queried: 8
total_jobs: 2767
top_3_jobs:
- @0@oss4:               {ops: 478773574, op: 37190314, cl: 98493350, mn: 23866435, ga: 263190243, sa: 26543810, gx: 13705173, sx: 191485, st: 2609984, sy: 12445667, rd: 220880, wr: 301677, pu: 14556}
- @0@oss1:               {ops: 440289919, op: 41416773, cl: 84440604, mn: 22883022, ga: 259247228, sa: 16171668, gx: 8088871, sx: 22344, st: 24, sy: 7681992, rd: 64480, wr: 269213, pu: 3700}
- 4668060@92097@n2cn0173:  {ops: 425923932, op: 57816778, cl: 138823088, mn: 12731051, ul: 12730440, mk: 15, mv: 5535867, ga: 85197150, sa: 22196216, gx: 34617241, sx: 34, sy: 4544474, rd: 38719, wr: 6162951, pu: 45529908}
...
```
### Run twice, calculate rate, show top 3 jobs:
```
# ./glljobstat.py -n 2 -c 3 -r
---
timestamp: 1693988935
query_duration: 13
servers_queried: 8
osts_queried: 24
mdts_queried: 8
total_jobs: 2732
top_3_job_operation_rates_during_query_windows:
- @33918@login:         {ops: 1917, op: 0, cl: 1014, mn: 0, ul: 0, mk: 0, ga: 903, sa: 0, gx: 0, sw: 13}
- 4692877@91469@comp1185:  {ops: 1859, op: 118, cl: 1179, mn: 50, ul: 62, mk: 0, mv: 62, ga: 300, sa: 4, gx: 54, rd: 0, wr: 28, pu: 3, sw: 13}
- 4693116@91469@comp1009:  {ops: 1791, op: 121, cl: 1177, mn: 63, ul: 37, mk: 0, mv: 49, ga: 272, sa: 3, gx: 66, rd: 0, wr: 0, pu: 3, sw: 13}
...
```
### Run once, show top 10 jobs, filter out jobids containing oss & login:
```
# ./glljobstat.py -n 1 -c 3 -f oss,login
---
timestamp: 1693989204
servers_queried: 8
osts_queried: 24
mdts_queried: 8
total_jobs: 2655
top_3_jobs:
- 4668060@92097@comp0173:  {ops: 426034658, op: 57832145, cl: 138857935, mn: 12734227, ul: 12733616, mk: 15, mv: 5537267, ga: 85220640, sa: 22201803, gx: 34626910, sx: 34, sy: 4545596, rd: 38719, wr: 6164498, pu: 45541253}
- 4668058@92097@comp0172:  {ops: 418195272, op: 56786160, cl: 136617854, mn: 12535103, ul: 12534492, mk: 15, mv: 5445887, ga: 82885212, sa: 21839731, gx: 34031784, sx: 34, sy: 4476846, rd: 85924, wr: 6126487, pu: 44829743}
- 4681703@92097@comp0229:  {ops: 279155542, op: 38637765, cl: 88256241, mn: 8089721, ul: 8089209, mk: 15, mv: 3520730, ga: 58473288, sa: 14113700, gx: 24290602, sx: 34, sy: 2885300, rd: 1285, wr: 3867380, pu: 28930272}
```
### Run once, show top 10 jobs, filter for jobids containing oss & login:
```
# ./glljobstat.py -n 1 -c 3 -f oss,login -fm
---
timestamp: 1693989291
servers_queried: 8
osts_queried: 24
mdts_queried: 8
total_jobs: 2670
top_3_jobs:
- @0@oss4:               {ops: 478777691, op: 37190544, cl: 98494321, mn: 23866665, ga: 263191958, sa: 26544360, gx: 13705403, sx: 191487, st: 2609984, sy: 12445757, rd: 220930, wr: 301726, pu: 14556}
- @0@oss1:               {ops: 440293163, op: 41416926, cl: 84441230, mn: 22883175, ga: 259249001, sa: 16172000, gx: 8089023, sx: 22344, st: 24, sy: 7682020, rd: 64494, wr: 269226, pu: 3700}
- @0@oss7:               {ops: 363854972, op: 16568386, cl: 45116913, mn: 8770914, ga: 36365047, sa: 102245054, gx: 9607769, sx: 48028, st: 21, sy: 42628119, rd: 30473311, wr: 65449947, pu: 6581463}
...
```
### Run once show show top 3 job ops in percentage to total ops:
```
# ./glljobstat.py -c 3 -n 1 -p
---
timestamp: 1693989365
servers_queried: 8
osts_queried: 24
mdts_queried: 8
total_jobs: 2662
top_3_job_operations_in_percent_to_total_operations:
- 4668058@92097@comp0172:  {ops: 5, op: 6, cl: 5, mn: 4, ul: 7, mk: 0, mv: 7, ga: 3, sa: 4, gx: 6, sx: 0, sy: 3, rd: 0, wr: 3, pu: 8}
- 4668060@92097@comp0173:  {ops: 5, op: 6, cl: 5, mn: 5, ul: 8, mk: 0, mv: 7, ga: 4, sa: 4, gx: 6, sx: 0, sy: 3, rd: 0, wr: 3, pu: 8}
- @0@oss4:               {ops: 5, op: 4, cl: 4, mn: 9, ga: 11, sa: 5, gx: 3, sx: 45, st: 4, sy: 8, rd: 0, wr: 0, pu: 0}
total_operations:
- ops:       {rate: 8774935283}
- cl:        {rate: 2558381972}
- ga:        {rate: 2379661320}
- op:        {rate: 978019848}
- pu:        {rate: 566309329}
- gx:        {rate: 540180883}
- sa:        {rate: 511439447}
- mn:        {rate: 279272377}
- rd:        {rate: 257793670}
- wr:        {rate: 238394981}
- ul:        {rate: 167359764}
- sy:        {rate: 160586870}
- mv:        {rate: 76740632}
- st:        {rate: 59900727}
- sx:        {rate: 429861}
- mk:        {rate: 262353}
- rm:        {rate: 195453}
- ln:        {rate: 5664}
- pa:        {rate: 132}
...
```
### Run forever show show top 3 jobs ops, total ops rate and highest op rate logged:
```
# ./glljobstat.py -c 3 -n 2 -tr
[...]
---
timestamp: 1693989469
query_duration: 13
servers_queried: 8
osts_queried: 24
mdts_queried: 8
total_jobs: 2540
top_3_job_operation_rates_during_query_windows:
- @33918@login5:         {ops: 2326, op: 0, cl: 1302, mn: 0, ul: 0, mk: 0, ga: 1024, sa: 0, gx: 0, sw: 13}
- 4693116@91469@comp1009:  {ops: 2013, op: 134, cl: 1335, mn: 62, ul: 49, mk: 0, mv: 62, ga: 297, sa: 4, gx: 66, rd: 0, wr: 0, pu: 4, sw: 13}
- 4692878@91469@comp0955:  {ops: 1814, op: 109, cl: 1199, mn: 49, ul: 49, mk: 0, mv: 49, ga: 271, sa: 3, gx: 53, rd: 0, wr: 28, pu: 3, sw: 13}
total_rate_per_operation_during_query_window:
- ops:       {rate: 30128}
- cl:        {rate: 13006}
- ga:        {rate: 6961}
- op:        {rate: 2469}
- pu:        {rate: 1911}
- gx:        {rate: 1575}
- sa:        {rate: 830}
- rd:        {rate: 824}
- wr:        {rate: 706}
- ul:        {rate: 683}
- mn:        {rate: 620}
- mv:        {rate: 413}
- sy:        {rate: 35}
- mk:        {rate: 0}
- sx:        {rate: 0}
- ln:        {rate: 0}
- rm:        {rate: 0}
- pa:        {rate: 0}
- st:        {rate: -4}
highest_rate_per_operation_in_logfile:
- ops:       {rate: 31160,     ts: 1693081240}
- rd:        {rate: 15744,     ts: 1693081240}
- cl:        {rate: 13006,     ts: 1693989469}
- ga:        {rate: 6961,      ts: 1693989469}
- op:        {rate: 3048,      ts: 1693563012}
- pu:        {rate: 1911,      ts: 1693989469}
- gx:        {rate: 1761,      ts: 1693563012}
- mn:        {rate: 1263,      ts: 1693563012}
- wr:        {rate: 1236,      ts: 1693081240}
- sa:        {rate: 1113,      ts: 1693740015}
- ul:        {rate: 917,       ts: 1693563012}
- mv:        {rate: 413,       ts: 1693989469}
- sy:        {rate: 361,       ts: 1693565717}
- mk:        {rate: 124,       ts: 1693563012}
- rm:        {rate: 46,        ts: 1693563012}
- qc:        {rate: 6,         ts: 1693801624}
- st:        {rate: 3,         ts: 1693081240}
- sx:        {rate: 0,         ts: 1693081240}
- ln:        {rate: 0,         ts: 1693081240}
- pa:        {rate: 0,         ts: 1693081240}
- gi:        {rate: 0,         ts: 1693563000}
job_with_hightest_rate_per_operation_in_logfile:
- ops:       {pu: 88, ops: 4451, st: 0, rd: 0, wr: 356, op: 509, cl: 753, mn: 470, ul: 412, mk: 86, rm: 46, mv: 0, ga: 1221, sa: 87, gx: 422, ts: 1693563012, job_id: 4681462@41671@comp0229}
- op:        {pu: 88, ops: 4451, st: 0, rd: 0, wr: 356, op: 509, cl: 753, mn: 470, ul: 412, mk: 86, rm: 46, mv: 0, ga: 1221, sa: 87, gx: 422, ts: 1693563012, job_id: 4681462@41671@comp0229}
- cl:        {rd: 34, ops: 1603, wr: 9, ga: 8, op: 0, cl: 1536, mn: 0, ul: 0, mk: 0, mv: 0, sa: 0, gx: 15, st: 0, pu: 0, ts: 1693081240, job_id: 4647531@57608@comp0502}
- mn:        {pu: 88, ops: 4451, st: 0, rd: 0, wr: 356, op: 509, cl: 753, mn: 470, ul: 412, mk: 86, rm: 46, mv: 0, ga: 1221, sa: 87, gx: 422, ts: 1693563012, job_id: 4681462@41671@comp0229}
- ln:        {rd: 77, ops: 472, wr: 55, pu: 0, ga: 118, sa: 0, op: 28, cl: 119, mn: 28, ln: 0, ul: 0, mk: 18, rm: 0, mv: 0, gx: 27, sx: 0, sy: 0, pa: 0, ts: 1693081240, job_id: @34013@login6}
- ul:        {pu: 88, ops: 4451, st: 0, rd: 0, wr: 356, op: 509, cl: 753, mn: 470, ul: 412, mk: 86, rm: 46, mv: 0, ga: 1221, sa: 87, gx: 422, ts: 1693563012, job_id: 4681462@41671@comp0229}
- mk:        {pu: 88, ops: 4451, st: 0, rd: 0, wr: 356, op: 509, cl: 753, mn: 470, ul: 412, mk: 86, rm: 46, mv: 0, ga: 1221, sa: 87, gx: 422, ts: 1693563012, job_id: 4681462@41671@comp0229}
- rm:        {pu: 88, ops: 4451, st: 0, rd: 0, wr: 356, op: 509, cl: 753, mn: 470, ul: 412, mk: 86, rm: 46, mv: 0, ga: 1221, sa: 87, gx: 422, ts: 1693563012, job_id: 4681462@41671@comp0229}
- mv:        {pu: 4, ops: 2013, rd: 0, wr: 0, op: 134, cl: 1335, mn: 62, ul: 49, mk: 0, mv: 62, ga: 297, sa: 4, gx: 66, ts: 1693989469, job_id: 4693116@91469@comp1009}
- ga:        {pu: 88, ops: 4451, st: 0, rd: 0, wr: 356, op: 509, cl: 753, mn: 470, ul: 412, mk: 86, rm: 46, mv: 0, ga: 1221, sa: 87, gx: 422, ts: 1693563012, job_id: 4681462@41671@comp0229}
- sa:        {op: 0, ops: 321, cl: 60, mn: 0, mk: 0, ga: 56, sa: 102, gx: 0, rd: 0, wr: 0, pu: 102, ts: 1693563307, job_id: 4665648@39362@comp0170}
- gx:        {pu: 88, ops: 4451, st: 0, rd: 0, wr: 356, op: 509, cl: 753, mn: 470, ul: 412, mk: 86, rm: 46, mv: 0, ga: 1221, sa: 87, gx: 422, ts: 1693563012, job_id: 4681462@41671@comp0229}
- sx:        {rd: 1, ops: 36, wr: 1, ga: 8, sa: 6, pu: 0, sy: 2, st: 0, op: 3, cl: 9, mn: 2, gx: 2, sx: 0, ts: 1693081240, job_id: @0@oss4}
- st:        {rd: 84, ops: 439, ga: 1, op: 22, cl: 276, mn: 0, ul: 0, mk: 0, rm: 0, sa: 0, gx: 0, wr: 52, st: 2, pu: 0, ts: 1693081240, job_id: 4647533@57608@comp1125}
- sy:        {pu: 97, ops: 811, sy: 75, rd: 3, wr: 28, sa: 30, op: 101, cl: 191, mn: 58, ul: 12, mk: 0, mv: 3, ga: 133, gx: 78, sx: 0, ts: 1693740002, job_id: 4681611@92097@comp0220}
- rd:        {rd: 2456, ops: 2531, wr: 75, sa: 0, pu: 0, ts: 1693081240, job_id: 4655776@92097@comp0160}
- wr:        {ga: 82, ops: 1451, rd: 80, wr: 372, op: 293, cl: 295, mn: 293, mk: 37, sa: 0, gx: 0, ts: 1693563012, job_id: 4667940@62567@comp0440}
- pu:        {pu: 400, ops: 1107, sy: 0, rd: 0, wr: 0, sa: 31, op: 113, cl: 327, mn: 22, ul: 20, mk: 0, mv: 0, ga: 143, gx: 52, sx: 0, ts: 1693802848, job_id: 4681704@92097@comp0229}
- gi:        {pu: 0, ops: 0, st: 0, rd: 0, wr: 0, gi: 0, op: 0, cl: 0, mn: 0, ul: 0, mk: 0, rm: 0, mv: 0, ga: 0, sa: 0, gx: 0, ts: 1693563000, job_id: 4680844@91469@comp0229}
- qc:        {qc: 6, ops: 114, ga: 45, gx: 64, ts: 1693801624, job_id: @0@login1}
- pa:        {rd: 77, ops: 472, wr: 55, pu: 0, ga: 118, sa: 0, op: 28, cl: 119, mn: 28, ln: 0, ul: 0, mk: 18, rm: 0, mv: 0, gx: 27, sx: 0, sy: 0, pa: 0, ts: 1693081240, job_id: @34013@login6}
...
```
