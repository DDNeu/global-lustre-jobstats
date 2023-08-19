# global-lustre-jobstats
glljobstat.py is based on [lljobstat](https://review.whamcloud.com/c/fs/lustre-release/+/48888/28/lustre/utils/lljobstat) with some enhencements!

## Enhancements:
* Aggreate stats over multiple OSS/MDS via SSH in parallel
* Calculate the rate of each job between queries
* Process returned strings to yaml like objects in parallel (3x faster)
* Use "naive" parsing to get another 3x speed up over ymal CLoader
* Filter for certain job_ids
* Filter out certain job_ids
* Config file for SSH, OSS/MDS & filter settings

This is still very rudimental!
A proper tool would allow filtering for specific OST/MDTs etc... patches welcome!

## Examples
### Help
```
(lljobstat) [root@n2oss1 bolausson]# ./glljobstat.py --help
usage: lljobstat [-h] [-c COUNT] [-i INTERVAL] [-n REPEATS] [--param PARAM]
                 [-o] [-m] [-s SERVERS] [--fullname] [--no-fullname]
                 [-f FILTER] [-fm] [-r]

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
  -r, --rate            Calculate the rate between two queries
```

### Run twice, calculate rate, show top 10 jobs:
```
(lljobstat) [root@n2oss1 bolausson]# ./glljobstat.py -n 2 -c 10 -r
---
timestamp: 1692372223
sample_duration: 50
top_jobs:
- 4614100@92097@n2lcn0141: {ops: 1312, rd: 1303, wr: 9}
- 4628146@39362@n2cn1144: {ops: 1008, op: 1, cl: 175, mn: 0, mk: 0, ga: 173, sa: 323, gx: 14, rd: 0, wr: 0, pu: 323}
- 4629707@62567@n2cn0167: {ops: 817, op: 1, cl: 155, mn: 0, mk: 0, ga: 172, sa: 243, gx: 1, rd: 0, wr: 0, pu: 246}
- 4628147@39362@n2cn1051: {ops: 761, op: 88, cl: 146, mn: 87, mk: 8, ga: 48, sa: 148, gx: 0, rd: 0, wr: 87, pu: 148}
- 4614091@92097@n2lcn0142: {ops: 724, rd: 710, wr: 14}
- 4601768@92097@n2cn0826: {ops: 615, op: 72, cl: 238, mn: 22, ul: 18, mk: 0, mv: 10, ga: 81, sa: 39, gx: 32, sx: 0, sy: 7, rd: 9, wr: 13, pu: 75}
- 4604904@92097@n2cn0357: {ops: 609, op: 70, cl: 235, mn: 20, ul: 21, mk: 0, mv: 9, ga: 81, sa: 38, gx: 29, sx: 0, sy: 7, rd: 9, wr: 14, pu: 74}
- 4613363@13313@n2gpu1223: {ops: 541, op: 110, cl: 110, mn: 0, ul: 0, mk: 0, ga: 212, sa: 0, gx: 0, rd: 109, wr: 0, pu: 0}
- 4604912@92097@n2cn0358: {ops: 483, op: 53, cl: 173, mn: 18, ul: 18, mk: 0, mv: 6, ga: 64, sa: 26, gx: 26, sx: 0, sy: 7, rd: 9, wr: 16, pu: 65}
- 4604911@92097@n2cn0358: {ops: 482, op: 56, cl: 196, mn: 14, ul: 12, mk: 0, mv: 9, ga: 61, sa: 32, gx: 22, sx: 0, sy: 0, rd: 6, wr: 15, pu: 59}
...
```
### Run once, show top 10 jobs:
```
(lljobstat) [root@n2oss1 bolausson]# ./glljobstat_testing.py -n 1 -c 10
---
timestamp: 1692222020
top_jobs:
- .0@n2oss4:       {ops: 450872221, op: 8985612, cl: 32847223, mn: 7556766, ga: 172015143, sa: 81582967, gx: 5249163, sx: 145539, st: 2610083, sy: 33185962, rd: 63191482, wr: 41084375, pu: 2417906}
- .0@n2oss8:       {ops: 434625228, op: 5558162, cl: 22671631, mn: 4582361, ga: 74530019, sa: 90323151, gx: 4700613, sx: 26447, st: 51, sy: 35836738, rd: 127631483, wr: 64743521, pu: 4021051}
- .0@n2oss7:       {ops: 381748393, op: 4711402, cl: 18595950, mn: 3751919, ga: 70785028, sa: 86948335, gx: 3856018, sx: 19001, st: 34, sy: 34442960, rd: 90835956, wr: 64186625, pu: 3615165}
- .0@n2oss6:       {ops: 330148115, op: 2840716, cl: 9532521, mn: 1922084, ga: 66039267, sa: 82328690, gx: 2003026, sx: 6707, st: 32, sy: 32568903, rd: 77375199, wr: 52368778, pu: 3162192}
- .0@n2oss3:       {ops: 322779945, op: 6862105, cl: 18542647, mn: 4678597, ga: 86738620, sa: 72757555, gx: 2337561, sx: 3175, st: 90, sy: 29707825, rd: 60881353, wr: 38174986, pu: 2095431}
- .0@n2oss5:       {ops: 309103508, op: 2701993, cl: 9064226, mn: 1830268, ga: 63758336, sa: 79464149, gx: 1895714, sx: 4405, st: 33, sy: 31654271, rd: 69391782, wr: 46532996, pu: 2805335}
- 4595658@13313@n2gpu1220: {ops: 277126435, op: 56221192, cl: 56269875, mn: 18, ul: 4, mk: 5, ga: 108150997, sa: 95905, gx: 100596, rd: 56088910, wr: 103028, pu: 95905}
- 4595669@13313@n2gpu1231: {ops: 273840101, op: 55552600, cl: 55601128, mn: 18, ul: 4, mk: 5, ga: 106870895, sa: 95877, gx: 100512, rd: 55420150, wr: 103035, pu: 95877}
- 4604904@92097@n2cn0357: {ops: 266363667, op: 30345092, cl: 100715391, mn: 9243875, ul: 9243302, mk: 14, mv: 4013542, ga: 35562890, sa: 16101573, gx: 13082712, sx: 34, sy: 3523806, rd: 4885237, wr: 6583294, pu: 33062905}
- .0@n2oss2:       {ops: 259294885, op: 3723859, cl: 9928806, mn: 2298045, ga: 58643542, sa: 70699421, gx: 1595170, sx: 1946, st: 105, sy: 28822779, rd: 45117278, wr: 36496902, pu: 1967032}
...
```
### Run once, show top 10 jobs, filter out jobids containing n2oss[1-8] & n2gpu1220:
```
(lljobstat) [root@n2oss1 bolausson]# ./glljobstat_testing.py -n 1 -c 10 -f n2oss1,n2oss2,n2oss3,n2oss4,n2oss5,n2oss6,n2oss7,n2oss8,n2gpu1220
---
timestamp: 1692222060
top_jobs:
- 4595669@13313@n2gpu1231: {ops: 273864315, op: 55557502, cl: 55606046, mn: 18, ul: 4, mk: 5, ga: 106880337, sa: 95885, gx: 100521, rd: 55425068, wr: 103044, pu: 95885}
- 4604904@92097@n2cn0357: {ops: 266383277, op: 30347233, cl: 100722289, mn: 9244572, ul: 9244089, mk: 14, mv: 4013759, ga: 35565490, sa: 16102610, gx: 13083767, sx: 34, sy: 3524180, rd: 4885705, wr: 6583793, pu: 33065742}
- 4595696@13313@n2gpu1217: {ops: 256451095, op: 51986732, cl: 52035195, mn: 16, ul: 3, mk: 5, ga: 100178344, sa: 95587, gx: 101004, rd: 51854818, wr: 103804, pu: 95587}
- 4604912@92097@n2cn0358: {ops: 233411322, op: 26562788, cl: 88287332, mn: 8097691, ul: 8097082, mk: 16, mv: 3520333, ga: 31146547, sa: 14115500, gx: 11918445, sx: 34, sy: 2890668, rd: 3714576, wr: 6101359, pu: 28958951}
- 4604911@92097@n2cn0358: {ops: 232533827, op: 26458097, cl: 87936453, mn: 8067651, ul: 8067057, mk: 16, mv: 3505595, ga: 31027449, sa: 14058008, gx: 11882882, sx: 34, sy: 2880922, rd: 3707110, wr: 6089814, pu: 28852739}
- 4601768@92097@n2cn0826: {ops: 205413209, op: 23343790, cl: 77632255, mn: 6970961, ul: 6970353, mk: 15, mv: 3144259, ga: 27621897, sa: 12504969, gx: 10564156, sx: 34, sy: 2416084, rd: 3690873, wr: 5639708, pu: 24913855}
- 4613363@13313@n2gpu1223: {ops: 191264129, op: 38768130, cl: 38805002, mn: 14, ul: 2, mk: 5, ga: 74723082, sa: 72343, gx: 76386, rd: 38668264, wr: 78558, pu: 72343}
- 4604900@92097@n2cn0357: {ops: 190203872, op: 21057319, cl: 69597550, mn: 6650749, ul: 6650140, mk: 19, mv: 2687887, ga: 25581843, sa: 10961013, gx: 10229352, sx: 34, sy: 2502786, rd: 4549603, wr: 5924075, pu: 23811502}
- 4615601@13313@n2gpu1214: {ops: 150693501, op: 30683350, cl: 30706364, mn: 14, ul: 2, mk: 5, ga: 58494946, sa: 45095, gx: 47662, rd: 30621939, wr: 49029, pu: 45095}
- 4604915@92097@n2cn0359: {ops: 145891997, op: 16148928, cl: 53107606, mn: 5089739, ul: 5089130, mk: 16, mv: 2046253, ga: 19675589, sa: 8354850, gx: 8446022, sx: 34, sy: 1921282, rd: 3104943, wr: 4684026, pu: 18223579}
...
```
### Run once, show top 10 jobs, filter for jobids containing n2oss[1-8] & n2gpu1220:
```
(lljobstat) [root@n2oss1 bolausson]# ./glljobstat_testing.py -n 1 -c 10 -f n2oss1,n2oss2,n2oss3,n2oss4,n2oss5,n2oss6,n2oss7,n2oss8,n2gpu1220 -fm
---
timestamp: 1692222101
top_jobs:
- .0@n2oss4:       {ops: 450884760, op: 8986310, cl: 32850266, mn: 7557387, ga: 172017724, sa: 81585692, gx: 5249789, sx: 145539, st: 2610083, sy: 33187060, rd: 63192060, wr: 41084939, pu: 2417911}
- .0@n2oss8:       {ops: 434638884, op: 5558948, cl: 22674985, mn: 4583048, ga: 74532696, sa: 90326136, gx: 4701305, sx: 26452, st: 51, sy: 35837910, rd: 127632139, wr: 64744159, pu: 4021055}
- .0@n2oss7:       {ops: 381762048, op: 4712171, cl: 18599239, mn: 3752594, ga: 70787794, sa: 86951350, gx: 3856694, sx: 19002, st: 34, sy: 34444171, rd: 90836592, wr: 64187241, pu: 3615166}
- .0@n2oss6:       {ops: 330161311, op: 2841469, cl: 9535757, mn: 1922748, ga: 66041853, sa: 82331610, gx: 2003691, sx: 6707, st: 32, sy: 32570059, rd: 77375814, wr: 52369378, pu: 3162193}
- .0@n2oss3:       {ops: 322791999, op: 6862782, cl: 18545602, mn: 4679201, ga: 86741035, sa: 72760165, gx: 2338168, sx: 3175, st: 90, sy: 29708895, rd: 60881917, wr: 38175536, pu: 2095433}
- .0@n2oss5:       {ops: 309116482, op: 2702725, cl: 9067416, mn: 1830920, ga: 63760907, sa: 79466982, gx: 1896369, sx: 4405, st: 33, sy: 31655419, rd: 69392386, wr: 46533582, pu: 2805338}
- 4595658@13313@n2gpu1220: {ops: 277173886, op: 56230816, cl: 56279507, mn: 18, ul: 4, mk: 5, ga: 108169522, sa: 95921, gx: 100613, rd: 56098512, wr: 103047, pu: 95921}
- .0@n2oss2:       {ops: 259315677, op: 3724537, cl: 9931795, mn: 2298656, ga: 58654643, sa: 70702032, gx: 1595781, sx: 1946, st: 105, sy: 28823869, rd: 45117834, wr: 36497447, pu: 1967032}
- .0@n2oss1:       {ops: 91816850, op: 5746868, cl: 13828360, mn: 3546209, ga: 48367908, sa: 9069627, gx: 1669877, sx: 3385, st: 170, sy: 3657138, rd: 3271902, wr: 2562619, pu: 92787}
- .994@n2gpu1220:  {ops: 25937, ga: 14092, st: 11845}
...
(lljobstat) [root@n2oss1 bolausson]#
```
