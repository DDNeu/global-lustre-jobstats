#!/bin/env python3

'''
lljobstat command. Read job_stats files, parse and aggregate data of every
job on multiple OSS/MDS via SSH using key or password, show top jobs and more
'''

import sys
import time
#import yaml
#import zaml
import signal
import argparse
import warnings
import configparser
from pathlib import Path
from getpass import getpass
from os.path import expanduser
from multiprocessing import Process, Queue
import urllib3

warnings.filterwarnings(action='ignore',module='.*paramiko.*')
urllib3.disable_warnings()

import paramiko # pylint: disable=wrong-import-position

signal.signal(signal.SIGINT, signal.default_int_handler)

class ArgParser: # pylint: disable=too-few-public-methods,too-many-instance-attributes
    '''
    Class to define lljobstat command arguments
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

    def run(self): # pylint: disable=too-many-statements
        '''
        define and parse arguments
        '''
        self.configfile = expanduser("~/.glljobstat.conf")

        parser = argparse.ArgumentParser(prog='lljobstat',
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
        parser.add_argument('-r', '--rate', dest='rate', action='store_true',
                            help='Calculate the rate between two queries')
        parser.add_argument('-l', '--length', dest='jobid_length',
                            help='Set job_id filename lenght for pretty printing')

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

        if self.args.rate and (self.args.repeats > 0 and self.args.repeats < 2):
            self.args.repeats = 2


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

    def __init__(self):
        self.args = None
        self.argparser = None
        self.hosts_param = None
        self.reference_time = None
        self.reference = {}

    def rate_calc(self, jobs, query_time):
        '''
        Class to calculate the rate between two queries
        '''
        jobrate = {}

        if not self.reference:
            self.reference = jobs
            self.reference_time = query_time
            duration = query_time
        else:
            duration = query_time - self.reference_time
            for job_id in self.reference:
                jobrate[job_id] = {}
                for metric in self.reference[job_id]:
                    if metric in self.op_keys.values():
                        old = self.reference[job_id][metric]
                        try:
                            new = jobs[job_id][metric]
                        except KeyError:
                            rate = 0
                        else:
                            difference = new - old
                            rate = round(difference / duration)
                        jobrate[job_id][metric] = rate
                    else:
                        jobrate[job_id][metric] = self.reference[job_id][metric]

            self.reference = jobs
            self.reference_time = query_time

        return jobrate, duration

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
                if "snapshot_time:" in line:
                    splitline = line.split()
                    key = splitline[0].strip(":")
                    value = int(splitline[1])
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

    def merge_job(self, jobs, job):
        '''
        merge stats data of job to jobs
        '''
        job2 = jobs.get(job['job_id'], {})

        for key in job.keys():
            if key not in self.op_keys.values():
                continue
            if job[key]['samples'] == 0:
                continue

            job2[key] = job2.get(key, 0) + job[key]['samples']
            job2['ops'] = job2.get('ops', 0) + job[key]['samples']

        job2['job_id'] = job['job_id']
        jobs[job['job_id']] = job2

    def insert_job_sorted(self, top_jobs, count, job):
        '''
        insert job to top_jobs in descending order by the key job['ops'].
        top_jobs is an array with at most count elements
        '''
        top_jobs.append(job)

        for i in range(len(top_jobs) - 2, -1, -1):
            if job['ops'] > top_jobs[i]['ops']:
                top_jobs[i + 1] = top_jobs[i]
                top_jobs[i] = job
            else:
                break

        if len(top_jobs) > count:
            top_jobs.pop()

    def pick_top_jobs(self, jobs, count):
        '''
        choose at most count elements from jobs, put them in an array in
        descending order by the key job['ops'].
        '''
        top_jobs = []
        for _, job in jobs.items():
            if self.args.fmod:
                if any(srv in str(job['job_id']) for srv in self.argparser.filter):
                    self.insert_job_sorted(top_jobs, count, job)
            else:
                if not any(srv in str(job['job_id']) for srv in self.argparser.filter):
                    self.insert_job_sorted(top_jobs, count, job)

        return top_jobs

    def print_job(self, job):
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

            opname = key
            if self.args.fullname:
                opname = self.op_keys[opname]

            print(f'{opname}: {job[val]}', end='')
            #print('%s: %d' % (opname, job[val]), end='')
            if first:
                first = False
        print('}')

    def print_top_jobs(self, top_jobs, total_jobs, count, timevalue):
        '''
        print top_jobs in YAML
        '''
        print('---') # mark the begining of YAML doc in stream
        print(f'timestamp: {int(time.time())}')
        if self.args.rate:
            print(f'sample_duration: {timevalue}')
        print(f'servers_queried: {len(self.argparser.serverlist)}')
        print(f'total_jobs: {total_jobs}')
        print(f'top_{count}_jobs:')
        for job in top_jobs:
            self.print_job(job)
        print('...') # mark the end of YAML doc in stream


    def run_once_par(self, query_type): # pylint: disable=too-many-locals
        '''
        scan/parse/aggregate/print top jobs in given job_stats pattern/path(s)
        '''
        jobs = {}

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
                self.merge_job(jobs, job)

        total_jobs = len(jobs)
        if self.args.rate and not self.reference:
            jobs, duration = self.rate_calc(jobs, query_time)
        elif self.args.rate and self.reference:
            jobs, duration = self.rate_calc(jobs, query_time)
            top_jobs = self.pick_top_jobs(jobs, self.args.count)
            self.print_top_jobs(top_jobs, total_jobs, self.args.count, duration)
        else:
            top_jobs = self.pick_top_jobs(jobs, self.args.count)
            self.print_top_jobs(top_jobs, total_jobs, self.args.count, query_time)

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

        #self.reference = {}
        #self.reference_time = ""

        self.hosts_param = self.get_data("param")

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
