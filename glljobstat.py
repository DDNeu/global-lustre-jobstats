#!/usr/bin/env python3

'''
glljobstat command. Read job_stats files, parse and aggregate data of every
job on multiple OSS/MDS via SSH using key or password, show top jobs and more
'''

import sys
import time
import signal
import pickle
import argparse
import warnings
import configparser
from pathlib import Path
from getpass import getpass
from os.path import expanduser
from collections import Counter
from multiprocessing import Process, Queue
import urllib3

warnings.filterwarnings(action='ignore',module='.*paramiko.*')
urllib3.disable_warnings()

import paramiko # pylint: disable=wrong-import-position

signal.signal(signal.SIGINT, signal.default_int_handler)

class ArgParser: # pylint: disable=too-few-public-methods,too-many-instance-attributes
    '''
    Class to define glljobstat command arguments
    and parse the real command line arguments.
    '''
    def __init__(self):
        self.args = None
        self.configfile = None
        self.config = None
        self.servers = None
        self.filter = None
        self.user = None
        self.key = None
        self.keytype = None
        self.serverlist = None
        self.jobid_length = None
        self.password = None
        self.totalratefile = None


    def run(self): # pylint: disable=too-many-statements,too-many-branches
        '''
        define and parse arguments
        '''
        self.configfile = expanduser("~/.glljobstat.conf")

        parser = argparse.ArgumentParser(prog='glljobstat.py',
                                         description='List top jobs.')
        parser.add_argument('-c', '--count', type=int, default=5,
                            help='the number of top jobs to be listed (default 5).')
        parser.add_argument('-i', '--interval', type=int, default=10,
                            help='the interval in seconds to check job stats again (default 10).')
        parser.add_argument('-n', '--repeats', type=int, default=-1,
                            help='the times to repeat the parsing (default unlimited).')
        parser.add_argument('--param', type=str, default='*.*.job_stats',
                            help='the param path to be checked (default *.*.job_stats).')
        parser.add_argument('-o', '--ost', dest='param', action='store_const',
                            const='obdfilter.*.job_stats',
                            help='check only OST job stats.')
        parser.add_argument('-m', '--mdt', dest='param', action='store_const',
                            const='mdt.*.job_stats',
                            help='check only MDT job stats.')
        parser.add_argument('-s', '--servers', dest='servers', type=str,
                            help='Comma separated list of OSS/MDS to query')
        parser.add_argument('--fullname', action='store_true', default=False,
                            help='show full operation name (default False).')
        parser.add_argument('--no-fullname', dest='fullname',
                            action='store_false',
                            help='show abbreviated operations name.')
        parser.add_argument('-f', '--filter', dest='filter', type=str,
                            help='Comma separated list of job_ids to ignore')
        parser.add_argument('-fm', '--fmod', dest='fmod', action='store_true',
                            help="""Modify the filter to only show job_ids that
                            match the filter instead of removing them""")
        parser.add_argument('-l', '--length', dest='jobid_length',
                            help='Set job_id filename lenght for pretty printing')
        parser.add_argument('-t', '--total', dest='total', action='store_true',
                            help='Show sum over all jobs for each operation')
        parser.add_argument('-tr', '--totalrate', dest='totalrate', action='store_true',
                            help="""Whenever -tr is is used, a persistent file
                            will be created and keep track of the highest rate ever""")
        parser.add_argument('-trf', '--totalratefile', dest='totalratefile', type=str,
                            default=expanduser("~/.glljobstatdb.pickle"),
                            help=f"""Path to a pickle file which will keep track of
                            the higest rate (default {expanduser("~/.glljobstatdb.pickle")})""")
        parser.add_argument('-p', '--percent', dest='percent', action='store_true',
                            help='Show top jobs in percentage to total ops')
        parser.add_argument('-ht', '--humantime', dest='humantime', action='store_true',
                            help='Show human readable time instead of timestamp')

        group = parser.add_argument_group('Mutually exclusive options')
        group_ex = group.add_mutually_exclusive_group()
        group_ex.add_argument('-d', '--dif', dest='difference', action='store_true',
                            help='Show change in counters between two queries')
        group_ex.add_argument('-r', '--rate', dest='rate', action='store_true',
                            help='Calculate the rate between two queries')

        self.args = parser.parse_args()
        self.config = configparser.ConfigParser()

        if not Path(self.configfile).is_file():
            self.config['SERVERS'] = {
                'list': "Comma separated list of OSS/MDS to query",
            }
            self.config['FILTER'] = {
                'list': "Comma separated list of job_ids to ignore",
            }
            self.config['MISC'] = {
                'jobid_length': 17,
                'totalratefile': expanduser("~/.glljobstat.db")
            }
            self.config['SSH'] = {
                'user': "SSH user to connect to OSS/MDS",
                'key': "Path to SSH key file to use, leave empty to usw a password",
                'keytype': "Key type used (DSS, DSA, ECDA, RSA, Ed25519)"
            }

            with open(self.configfile, 'w', encoding='utf-8') as cfg:
                self.config.write(cfg)
                print(f'Example configuration file {self.configfile} created!')
                sys.exit()
        else:
            self.config.read(self.configfile)

        if self.args.servers:
            self.servers = set(self.args.servers.split(","))
        else:
            self.servers = {i.strip() for i in self.config['SERVERS']['LIST'].split(",") if i != ''}

        if self.args.filter:
            self.filter = set(self.args.filter.split(","))
        else:
            self.filter = {i.strip() for i in self.config['FILTER']['LIST'].split(",") if i != ''}

        if self.args.totalratefile:
            self.totalratefile = self.args.totalratefile
        else:
            self.totalratefile = self.config['MISC']['totalratefile']

        if self.args.jobid_length:
            self.jobid_length = int(self.args.jobid_length)
        else:
            try:
                self.jobid_length = int(self.config['MISC']['jobid_length'])
            except Exception: # pylint: disable=bare-except,broad-exception-caught
                self.jobid_length = 17

        self.serverlist = set(self.servers)
        self.user = self.config['SSH']['user']
        self.keytype = self.config['SSH']['keytype']

        if self.config['SSH']['key']:
            self.key = self.config['SSH']['key']
        else:
            self.password = getpass()

        if self.args.totalrate:
            self.args.rate = True
            self.args.total = True
            self.args.difference = False
            self.args.percent = False

        if (self.args.rate or self.args.difference) and (self.args.repeats > 0 and
                                                        self.args.repeats < 2):
            self.args.repeats = 2

        if self.args.percent:
            self.args.total = True


class JobStatsParser:
    '''
    Class to get/parse/aggregate/sort/print top jobs in job_stats
    '''
    op_keys = {
        'ops': 'ops',
        'cr' : 'create',
        'op' : 'open',
        'cl' : 'close',
        'mn' : 'mknod',
        'ln' : 'link',
        'ul' : 'unlink',
        'mk' : 'mkdir',
        'rm' : 'rmdir',
        'mv' : 'rename',
        'ga' : 'getattr',
        'sa' : 'setattr',
        'gx' : 'getxattr',
        'sx' : 'setxattr',
        'st' : 'statfs',
        'sy' : 'sync',
        'rd' : 'read',
        'wr' : 'write',
        'pu' : 'punch',
        'mi' : 'migrate',
        'fa' : 'fallocate',
        'dt' : 'destroy',
        'gi' : 'get_info',
        'si' : 'set_info',
        'qc' : 'quotactl',
        'pa' : 'prealloc'
    }

    op_keys_rev = {
        'ops'      : 'ops',
        'create'   : 'cr',
        'open'     : 'op',
        'close'    : 'cl',
        'mknod'    : 'mn',
        'link'     : 'ln',
        'unlink'   : 'ul',
        'mkdir'    : 'mk',
        'rmdir'    : 'rm',
        'rename'   : 'mv',
        'getattr'  : 'ga',
        'setattr'  : 'sa',
        'getxattr' : 'gx',
        'setxattr' : 'sx',
        'statfs'   : 'st',
        'sync'     : 'sy',
        'read'     : 'rd',
        'write'    : 'wr',
        'punch'    : 'pu',
        'migrate'  : 'mi',
        'fallocate': 'fa',
        'destroy'  : 'dt',
        'get_info' : 'gi',
        'set_info' : 'si',
        'quotactl' : 'qc',
        'prealloc' : 'pa'
    }

    misc_keys = {
        'sw' : 'sampling_window',
        'sn' : 'snapshot_time',
        'ts' : 'timestamp',
    }

    def __init__(self):
        self.args = None
        self.argparser = None
        self.hosts_param = None
        self.osts_mdts = None
        self.reference_time = None
        self.reference_snaptime = None
        self.reference = {}

    def topdb(self, total_ops, jobs, query_time): # pylint: disable=too-many-locals,too-many-branches,too-many-statements
        '''
        Class to upate/read the ever highest ops rate picks
        '''
        topdbdict = {"top_job_per_op": {}, "top_ops": {}}

        try:
            with open(self.args.totalratefile, 'rb') as picf:
                oldjobs = pickle.load(picf)
        except FileNotFoundError:
            with open(self.args.totalratefile, 'wb') as picf:
                pickle.dump(topdbdict, picf, pickle.HIGHEST_PROTOCOL)
            return False

        try:
            top_jop_op = oldjobs["top_job_per_op"]
        except KeyError:
            top_jop_op = {}

        for opkey in self.op_keys:
            longopkey = self.op_keys[opkey]

            try:
                top_jop_op[longopkey]
            except KeyError:
                top_jop_op.update({longopkey: {}})

            for jobkey in jobs.keys():
                if longopkey in jobs[jobkey].keys():
                    try:
                        toprate = top_jop_op[longopkey][longopkey]
                    except KeyError:
                        top_jop_op[longopkey] = jobs[jobkey]
                        top_jop_op[longopkey].update({"timestamp": query_time})
                        continue

                    rate = jobs[jobkey][longopkey]

                    if toprate < rate:
                        top_jop_op[longopkey] = jobs[jobkey]
                        top_jop_op[longopkey].update({"timestamp": query_time})

        topdbdict["top_job_per_op"] = top_jop_op

        # -----------------------------------------------------------------------------------------

        newtopops = {}
        stamped_ops = {}

        for ops in total_ops.keys():
            stamped_ops.update({ops: {"rate": total_ops[ops], "timestamp": query_time}})

        print(oldjobs.keys())
        for ops in oldjobs["top_ops"].keys():
            oldops = oldjobs["top_ops"][ops]["rate"]
            oldts = oldjobs["top_ops"][ops]["timestamp"]

            try:
                newops = stamped_ops[ops]["rate"]
            except KeyError:
                newtopops.update({ops: {"rate": oldops, "timestamp": oldts}})
                continue

            newts = stamped_ops[ops]["timestamp"]
            if newops > oldops:
                newtopops.update({ops:{"rate": newops, "timestamp": newts}})
            else:
                newtopops.update({ops: {"rate": oldops, "timestamp": oldts}})

            stamped_ops.pop(ops, None)

        if stamped_ops:
            newtopops.update(stamped_ops)


        topdbdict["top_ops"] = newtopops

        with open(self.args.totalratefile, 'wb') as picf:
            pickle.dump(topdbdict, picf, pickle.HIGHEST_PROTOCOL)

        return topdbdict


    def rate_calc(self, jobs, query_time, timestamp_dict): # pylint: disable=too-many-branches,too-many-locals
        '''
        Class to calculate the rate between two queries
        '''
        jobrate = {}
        job_sampling_window = {}

        if not self.reference: # pylint: disable=too-many-nested-blocks
            self.reference = jobs
            self.reference_time = query_time
            self.reference_snaptime = timestamp_dict
            query_duration = 0
        else:
            query_duration = query_time - self.reference_time
            for job_id in self.reference:
                jobrate[job_id] = {}
                job_sampling_window[job_id] = {}

                try:
                    job_snap_time_new = timestamp_dict[job_id]["snapshot_time"]
                except KeyError:
                    continue

                try:
                    job_snap_time_ref = self.reference_snaptime[job_id]["snapshot_time"]
                except KeyError:
                    continue

                job_snap_time_new = timestamp_dict[job_id]["snapshot_time"]
                duration = job_snap_time_new - job_snap_time_ref
                job_sampling_window[job_id] = duration

                for metric in self.reference[job_id]:
                    if metric in self.op_keys.values():
                        old = self.reference[job_id][metric]
                        try:
                            new = jobs[job_id][metric]
                        except KeyError:
                            rate = 0
                        else:
                            dif = new - old
                            if self.args.rate:
                                if duration == 0:
                                    rate = 0
                                else:
                                    rate = round(dif / duration)
                            if self.args.difference:
                                rate = dif
                        jobrate[job_id][metric] = rate
                    else:
                        jobrate[job_id][metric] = self.reference[job_id][metric]

            self.reference = jobs
            self.reference_snaptime = timestamp_dict
            self.reference_time = query_time

        return jobrate, job_sampling_window, query_duration


    def pct_calc(self, jobs, total_ops):
        '''
        Class to calc job obs percentage against total ops
        '''
        jobpct = {}

        for job_id in jobs:
            jobpct[job_id] = {}
            for metric in jobs[job_id]:
                if metric in self.op_keys.values():
                    try:
                        j_ops = jobs[job_id][metric]
                    except KeyError:
                        pct = 0
                    else:
                        t_ops = total_ops[metric]
                        if t_ops == 0:
                            pct = 0
                        else:
                            pct = j_ops * 100 / t_ops
                    jobpct[job_id][metric] = round(pct)
                else:
                    jobpct[job_id][metric] = jobs[job_id][metric]

        return jobpct


    def total_calc(self, jobs):
        '''
        Class to the sum of all jobs for each operation
        '''
        total_dict = {}

        for job_id in jobs:
            for metric in jobs[job_id]:
                if metric in self.op_keys.values():
                    try:
                        total_dict[metric] += jobs[job_id][metric]
                    except KeyError:
                        total_dict.update({metric: jobs[job_id][metric]})

        return total_dict


    def parse_single_job_stats_beo(self, queue, data): # pylint: disable=too-many-locals
        '''
        parse it manually into a dict
        '''
        data_iterable = iter(data[0].splitlines())
        jobstats_dict = {"job_stats": []}

        for line in data_iterable:
            try:
                if line == "job_stats:":
                    continue
                if "- job_id:" in line:
                    job_dict = {}
                    splitline = line.split()
                    key = splitline[1].rstrip(":")
                    value = splitline[2]
                    job_dict.update({key: value})
                    continue
                if any(timekey in line for timekey in ['snapshot_time:',
                                                        'start_time:',
                                                        'elapsed_time:']):
                    splitline = line.split()
                    key = splitline[0].strip(":")
                    #Remove trailing .nsecs value from snapshot_time introduced in Lustre 2.15
                    value = int(splitline[1].split('.', 1)[0])
                    job_dict.update({key: value})
                    continue
                while not "- job_id:" in line:
                    clean = line.replace(' ', '')
                    metric_raw, values_raw = clean.split("{")
                    metric = metric_raw.rstrip(":")
                    value_list = values_raw.rstrip("}").split(",")
                    metrics_dict = {metric: {}}

                    for item in value_list:
                        value_desc, value_counter_raw = item.split(":")

                        try:
                            value_counter = int(value_counter_raw)
                        except ValueError:
                            value_counter = value_counter_raw

                        metrics_dict[metric].update({value_desc: value_counter})

                    job_dict.update(metrics_dict)
                    line = next(data_iterable)
                jobstats_dict["job_stats"].append(job_dict)
                if "- job_id:" in line:
                    job_dict = {}
                    splitline = line.split()
                    key = splitline[1].rstrip(":")
                    value = splitline[2]
                    job_dict.update({key: value})
                    continue
            except StopIteration:
                jobstats_dict["job_stats"].append(job_dict)
                break

        queue.put(jobstats_dict)


    def merge_job(self, jobs, job, timestamp_dict):
        '''
        merge stats data of job to jobs
        '''
        jobid = job['job_id']
        job2 = jobs.get(job['job_id'], {})
        timestamp_dict[jobid] = {}

        include_metrics = list(self.op_keys.values())
        timestamp_id = ["snapshot_time", "start_time", "elapsed_time"]

        for key in job.keys():
            if key in timestamp_id:
                timestamp_dict[jobid].update({key: job[key]})
            if key not in include_metrics:
                continue
            if job[key]['samples'] == 0:
                continue

            job2[key] = job2.get(key, 0) + job[key]['samples']
            job2['ops'] = job2.get('ops', 0) + job[key]['samples']

        job2['job_id'] = jobid
        jobs[jobid] = job2

    def insert_job_sorted(self, top_jobs, count, job):
        '''
        insert job to top_jobs in descending order by the key job['ops'].
        top_jobs is an array with at most count elements
        '''
        top_jobs.append(job)

        for i in range(len(top_jobs) - 2, -1, -1):

            try:
                if job['ops'] > top_jobs[i]['ops']:
                    top_jobs[i + 1] = top_jobs[i]
                    top_jobs[i] = job
                else:
                    break
            except KeyError:
                pass

        if len(top_jobs) > count:
            top_jobs.pop()


    def pick_top_jobs(self, jobs, count):
        '''
        choose at most count elements from jobs, put them in an array in
        descending order by the key job['ops'].
        '''
        top_jobs = []
        for _, job in jobs.items():
            if not any(val != 0 and isinstance(val, int) for val in job.values()):
                continue
            if self.args.fmod:
                if any(srv in str(job['job_id']) for srv in self.argparser.filter):
                    self.insert_job_sorted(top_jobs, count, job)
            else:
                if not any(srv in str(job['job_id']) for srv in self.argparser.filter):
                    self.insert_job_sorted(top_jobs, count, job)

        return top_jobs


    def print_job(self, job, sampling_window):
        '''
        print single job
        '''
        #print('- %-16s {' % (job['job_id'] + ':'), end='')
        print(f'- {job["job_id"] + ":" : <{self.argparser.jobid_length}}{{', end='')
        first = True
        for key, val in self.op_keys.items():
            if not val in job.keys():
                continue
            if not first:
                print(", ", end='')

            op_name = key
            if self.args.fullname:
                op_name = self.op_keys[op_name]

            print(f'{op_name}: {job[val]}', end='')
            #print('%s: %d' % (opname, job[val]), end='')
            if first:
                first = False
        if self.args.fullname:
            sw_name = self.misc_keys['sw']
        else:
            sw_name = 'sw'
        if sampling_window:
            print(f', {sw_name}: {sampling_window}', end='')
        print('}')


    def print_top_jobs(self, # pylint: disable=too-many-arguments
                        top_jobs,
                        total_jobs,
                        count,
                        job_sampling_window,
                        query_time, query_duration):
        '''
        print top_jobs in YAML
        '''
        if self.args.humantime:
            times = time.strftime("%a %d %b %Y %H-%M-%S +0000", time.localtime(query_time))
        else:
            times = query_time

        print('---') # mark the begining of YAML doc in stream
        #print(f'timestamp: {int(time.time())}')
        print(f'timestamp: {times}')
        if self.args.rate or self.args.difference:
            print(f'query_duration: {query_duration}')
        print(f'servers_queried: {len(self.argparser.serverlist)}')
        print(f'osts_queried: {self.osts_mdts["obdfilter"]}')
        print(f'mdts_queried: {self.osts_mdts["mdt"]}')
        print(f'total_jobs: {total_jobs}')
        if self.args.percent:
            print(f'top_{count}_job_pct:', end="")
        elif self.args.rate or self.args.difference:
            print(f'top_{count}_job_rates:', end="")
        else:
            print(f'top_{count}_jobs:', end="")
        if not top_jobs:
            print(' []')
        else:
            print('')
            for job in top_jobs:
                if job_sampling_window:
                    sampling_window = job_sampling_window[job['job_id']]
                else:
                    sampling_window = False
                self.print_job(job, sampling_window)
        if not (self.args.total or self.args.totalrate or self.args.percent):
            print('...') # mark the end of YAML doc in stream


    def print_metric(self, ops):
        '''
        print single metric
        '''
        for key in dict(sorted(ops.items(), key=lambda item: item[1], reverse=True)):
            if self.args.fullname:
                op_name = key
            else:
                op_name = self.op_keys_rev[key]
#            print(f'- {opname + ":" : <10}{ops[key]}')
            print(f'- {op_name + ":" : <10} {{rate: {str(ops[key])}}}')

    def print_total_ops_logged_metric(self, ops):
        '''
        print single metric from total_ops_logged
        '''
        rates = {}
        for item in ops.keys():
            rates.update({item: ops[item]["rate"]})

        for key in dict(sorted(rates.items(), key=lambda item: item[1], reverse=True)):
            if self.args.humantime:
                times = time.strftime("%a %d %b %Y %H-%M-%S +0000",
                                    time.localtime(ops[key]["timestamp"]))
            else:
                times = ops[key]["timestamp"]

            rate = rates[key]

            if self.args.fullname:
                op_name = key
                ts_name = "timestamp"
            else:
                op_name = self.op_keys_rev[key]
                ts_name = "ts"
            print(f'- {op_name + ":" : <10} {{rate: {str(rate) + "," : <10} {ts_name}: {times}}}')

    def print_total_ops_logged_metric_job(self, ops): # pylint: disable=too-many-branches
        '''
        print single job info for highest op rate logged
        '''
        for op_key in ops.keys():
            if self.args.fullname:
                op_name = op_key
                ts_name = "timestamp"
            else:
                # op_name =(list(self.op_keys.keys())[list(self.op_keys.values()).index(op_key)])
                op_name = self.op_keys_rev[op_key]
                ts_name = "ts"

            num_items = len(ops[op_key].keys()) - 2
            counter = 1


            if not "job_id" in ops[op_key]:
                continue

            print(f'- {op_name + ":" : <10} {{', end='')
            # print(f'- {op_name + ":" : <10} {{job_id: {ops[op_key]["job_id"]}, ', end='')

            #print(f'- {op_name}:', end='')
            for item in ops[op_key].keys():
                if item in self.op_keys.values():
                    if self.args.fullname:
                        item_name = self.op_keys_rev[item]
                    else:
                        item_name = item
                    if counter == 1:
                        print(f'{item_name}: {ops[op_key][item]}, ', end='')
                    elif counter > 1 and counter < num_items:
                        print(f'{item_name}: {ops[op_key][item]}, ', end='')
                    else:
                        if self.args.humantime:
                            times = time.strftime("%a %d %b %Y %H-%M-%S +0000",
                                        time.localtime(ops[op_key]["timestamp"]))
                        else:
                            times = ops[op_key]["timestamp"]
                        print(f'{item_name}: {ops[op_key][item]}, {ts_name}: {times}, ', end='')
                        print(f'job_id: {ops[op_key]["job_id"]}', end='')
                    counter += 1
            print('}')

    def print_total_ops(self, total_ops): # pylint: disable=too-many-arguments
        '''
        print total ops in YAML
        '''
        if self.args.rate:
            print('total_op_rate:')
        else:
            print('total_ops:')
        self.print_metric(total_ops)
        if not self.args.totalrate:
            print('...') # mark the end of YAML doc in stream

    def print_total_ops_logged(self, total_ops_logged):
        '''
        print total highest ops ever in YAML
        '''
        print('total_op_rate_logged:')
        self.print_total_ops_logged_metric(total_ops_logged["top_ops"])

        print('top_job_per_op_logged:')
        self.print_total_ops_logged_metric_job(total_ops_logged["top_job_per_op"])
        print('...') # mark the end of YAML doc in stream

    def run_once_par(self, query_type): # pylint: disable=too-many-locals,too-many-branches
        '''
        scan/parse/aggregate/print top jobs in given job_stats pattern/path(s)
        '''
        jobs = {}
        timestamp_dict = {}

        query_time = int(time.time())
        #ssh_start = time.time()
        statsdata = self.get_data(query_type)
        #ssh_stop = time.time()
        #ssh_time = ssh_stop - ssh_start

        #parser_start = time.time()
        objs = []
        procs = []
        proc_q = Queue()

        try:
            for data in statsdata:
                proc = Process(target=self.parse_single_job_stats_beo, args=(proc_q, [data]))
                procs.append(proc)
                proc.start()
            for proc in procs:
                ret = proc_q.get() # blocking
                objs.append(ret)
            for proc in procs:
                proc.join()
        except Exception as exn: # pylint: disable=bare-except,broad-exception-caught
            print(exn)
            sys.exit()

        #parser_stop = time.time()
        #parser_time = parser_stop - parser_start

        #print(f"SSH time         : {ssh_time}")
        #print(f"Parser time      : {parser_time}")

        for obj in objs:
            if obj['job_stats'] is None:
                continue

            for job in obj['job_stats']:
                self.merge_job(jobs, job, timestamp_dict)

        total_jobs = len(set(jobs))

        if (self.args.rate or self.args.difference) and not self.reference:
            jobs, job_sampling_window, query_duration = self.rate_calc(jobs,
                                                                        query_time,
                                                                        timestamp_dict)
        elif (self.args.rate or self.args.difference) and self.reference:
            jobs, job_sampling_window, query_duration = self.rate_calc(jobs,
                                                                        query_time,
                                                                        timestamp_dict)
            if self.args.total or self.args.percent or self.args.totalrate:
                total_ops = self.total_calc(jobs)
            if self.args.totalrate and self.args.total:
                top_ops_ever = self.topdb(total_ops, jobs, query_time)
            if self.args.percent:
                jobs = self.pct_calc(jobs, total_ops)
            top_jobs = self.pick_top_jobs(jobs, self.args.count)
            self.print_top_jobs(top_jobs,
                                total_jobs,
                                self.args.count,
                                job_sampling_window,
                                query_time,
                                query_duration)
            if self.args.total:
                self.print_total_ops(total_ops)
            if self.args.totalrate and top_ops_ever:
                self.print_total_ops_logged(top_ops_ever)
        else:
            if self.args.total or self.args.percent:
                total_ops = self.total_calc(jobs)
            if self.args.percent:
                jobs = self.pct_calc(jobs, total_ops)
            top_jobs = self.pick_top_jobs(jobs, self.args.count)
            self.print_top_jobs(top_jobs, total_jobs, self.args.count, 0, query_time, 0)
            if self.args.total:
                self.print_total_ops(total_ops)


    def run_once_retry(self, query_type): #pylint: disable=inconsistent-return-statements
        '''
        Call run_once. If run_once succeeds, return.
        If run_once throws an exception, retry for few times.
        '''
        for i in range(2, -1, -1):  # 2, 1, 0
            try:
                return self.run_once_par(query_type)
            except Exception: # pylint: disable=bare-except,broad-exception-caught
                if i == 0:
                    raise


    def ssh_get(self, queue, host, query_type, cmd): #pylint: disable=inconsistent-return-statements
        '''
        SSH to all servers and execute lctl command
        '''
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            if self.argparser.password:
                ssh.connect(hostname=host,
                            username=self.argparser.user,
                            password=self.argparser.password)

            else:
                if self.argparser.keytype in ["DSS", "DSA"]:
                    ssh_pkey = paramiko.DSSKey.from_private_key_file(
                        filename=self.argparser.key)

                if self.argparser.keytype == "ECDSA":
                    ssh_pkey = paramiko.ECDSAKey.from_private_key_file(
                        filename=self.argparser.key)

                if self.argparser.keytype == "RSA":
                    ssh_pkey = paramiko.RSAKey.from_private_key_file(
                        filename=self.argparser.key)

                if self.argparser.keytype == "Ed25519":
                    ssh_pkey = paramiko.Ed25519Key.from_private_key_file(
                        filename=self.argparser.key)

                ssh.connect(hostname=host,
                            username=self.argparser.user,
                            pkey=ssh_pkey)

            try:
                stdin, stdout, stderr = ssh.exec_command(cmd)
            except Exception as exn: # pylint: disable=bare-except,broad-exception-caught
                ssh.close()
                print(exn)
                error = stderr.read().decode(encoding='UTF-8') # pylint: disable=used-before-assignment
                print(error)
                stdinput = stdin.read().decode(encoding='UTF-8') # pylint: disable=used-before-assignment
                print(stdinput)
                return "Exception running ssh.exec_command"

            output = stdout.read().decode(encoding='UTF-8')

            if query_type == "param":
                hostparam = {host: output.split()}
                queue.put(hostparam)
            if query_type == "stats":
                queue.put(output)

        except KeyboardInterrupt:
            print('Received KeyboardInterrupt in run()')
            sys.exit()


    def get_data(self, query_type):
        '''
        Spawn SSH connections to each server in parallel to gather data
        '''
        if query_type == "param":
            hostdata = {}
        if query_type == "stats":
            hostdata = []

        procs = []
        proc_q = Queue()

        try:
            for host in self.argparser.serverlist:
                if query_type == "param":
                    cmd = f'lctl list_param {self.args.param}'
                    proc = Process(target=self.ssh_get, args=(proc_q, host, query_type, cmd))
                    procs.append(proc)
                    proc.start()
                if query_type == "stats":
                    for param in self.hosts_param[host]:
                        cmd = f'lctl get_param -n {param}'
                        proc = Process(target=self.ssh_get, args=(proc_q, host, query_type, cmd))
                        procs.append(proc)
                        proc.start()

            for proc in procs:
                ret = proc_q.get() # blocking
                if query_type == "param":
                    hostdata.update(ret)
                if query_type == "stats":
                    hostdata.append(ret)
            for proc in procs:
                proc.join()
        except Exception as exn: # pylint: disable=bare-except,broad-exception-caught
            print(exn)
            sys.exit()
        else:
            return hostdata


    def Run(self):
        '''
        run task periodically or for some times with given interval
        '''
        self.argparser = ArgParser()
        self.argparser.run()
        self.args = self.argparser.args

        self.hosts_param = self.get_data("param")
        self.osts_mdts = Counter([item.split('.')[0] for
                        sublist in self.hosts_param.values() for
                        item in sublist])

        i = 0
        try:
            while True:
                self.run_once_retry("stats")
                i += 1
                if self.args.repeats != -1 and i >= self.args.repeats:
                    break
                time.sleep(self.args.interval)
        except KeyboardInterrupt:
            print("\nReceived KeyboardInterrupt - stopping")
            sys.exit()


if __name__ == "__main__":
    JobStatsParser().Run()
