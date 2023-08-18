#!/bin/env python3

'''
lljobstat command. Read job_stats files, parse and aggregate data of every
job on multiple OSS/MDS, show top jobs
'''

import argparse
import errno
import subprocess
import sys
import time
import yaml
#import zaml
import signal
import urllib3
import warnings
import configparser
from multiprocessing import Process, Queue, Pool, Manager, active_children, Pipe
from subprocess import Popen, PIPE, STDOUT
from pprint import pprint
from os.path import expanduser
from pathlib import Path

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    print("Install libyaml-dev for faster processing using ", file=sys.stderr)
    from yaml import Loader, Dumper

warnings.filterwarnings(action='ignore',module='.*paramiko.*')
urllib3.disable_warnings()

import paramiko

signal.signal(signal.SIGINT, signal.default_int_handler)

FILTER = ""
FMOD = False
RATE = False
REFERENCE = {}
REFERENCE_TIME = ""

class ArgParser: # pylint: disable=too-few-public-methods
    '''
    Class to define lljobstat command arguments
    and parse the real command line arguments.
    '''
    def __init__(self):
        self.args = None

    def run(self):
        '''
        define and parse arguments
        '''
        self.CONFIGFILE = expanduser("~/.glljobstat.conf")

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
                            help='Modify the filter to only show job_ids that match the filter instead of removing them')
        parser.add_argument('-r', '--rate', dest='rate', action='store_true',
                            help='Calculate the rate between two queries')

        self.args = parser.parse_args()
        self.config = configparser.ConfigParser()
        
        if not Path(self.CONFIGFILE).is_file():
            self.config['SERVERS'] = {
                'list': "Comma separated list of OSS/MDS to query",
            }
            self.config['FILTER'] = {
                'list': "Comma separated list of job_ids to ignore",
            }
            self.config['SSH'] = {
                'user': "SSH user to connect to OSS/MDS",
                'key': "Path to SSH key file to use",
                'keytype': "Key type used (DSS, DSA, ECDA, RSA, Ed25519)"
            }

            with open(self.CONFIGFILE, 'w') as f:
                self.config.write(f)
                print(f'Example configuration file {self.CONFIGFILE} created!')
                sys.exit()
        else:
            self.config.read(self.CONFIGFILE)

        if self.args.servers:
            self.servers = self.args.servers.split(",")
        else:
            self.servers = [i.strip() for i in self.config['SERVERS']['LIST'].split(",") if i != '']

        if self.args.filter:
            self.filter = self.args.filter.split(",")
        else:
            self.filter = [i.strip() for i in self.config['FILTER']['LIST'].split(",") if i != '']
        
        global FILTER
        FILTER = set(self.filter)

        self.user = self.config['SSH']['user']
        self.key = self.config['SSH']['key']
        self.keytype = self.config['SSH']['keytype']
        self.serverlist = set(self.servers)
        
        global FMOD
        self.fmod = self.args.fmod
        FMOD = self.fmod
        
        global HOSTS
        HOSTS = self.serverlist
        
        global RATE
        RATE = self.args.rate
        
        if RATE and (self.args.repeats > 0 and self.args.repeats < 2):
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

    def RateCalc(self, jobs, QUERY_TIME):
        '''
        Class to calculate the rate between two queries
        '''
        global REFERENCE
        global REFERENCE_TIME
        
        jobrate = {}
        
        if not REFERENCE:
            REFERENCE = jobs
            REFERENCE_TIME = QUERY_TIME
            duration = QUERY_TIME
        else:
            duration = QUERY_TIME - REFERENCE_TIME
            for job_id in REFERENCE:
                jobrate[job_id] = {}
                for metric in REFERENCE[job_id]:
                    if metric in self.op_keys.values():
                        old = REFERENCE[job_id][metric]
                        try:
                            new = jobs[job_id][metric]
                        except KeyError:
                            rate = 0
                        else:
                            difference = new - old
                            rate = round(difference / duration)
                        jobrate[job_id][metric] = rate
                    else:
                        jobrate[job_id][metric] = REFERENCE[job_id][metric]

            REFERENCE = jobs
            REFERENCE_TIME = QUERY_TIME

        return jobrate, duration

    def parse_single_job_stats(self, queue, data): # pylint: disable=no-self-use
        '''
        read single job_stats file, parse it and return an object
        '''
        output = data[0].replace('job_id:          @', f'job_id:          .')

        try:
            yaml_obj = yaml.load(output, Loader=Loader)  # need several seconds...
            #yaml_obj = zaml.load(output)
        except yaml.scanner.ScannerError:
            # only print the file name here
            print("failed to parse the content of %s" % param, file=sys.stdout)
            raise

        queue.put(yaml_obj)

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

    def insert_job_sorted(self, top_jobs, count, job): # pylint: disable=no-self-use
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
            if FMOD:
                if any(srv in str(job['job_id']) for srv in FILTER):
                    self.insert_job_sorted(top_jobs, count, job)
            else:
                if not any(srv in str(job['job_id']) for srv in FILTER):
                    self.insert_job_sorted(top_jobs, count, job)

        return top_jobs

    def print_job(self, job):
        '''
        print single job
        '''
        print('- %-16s {' % (job['job_id'] + ':'), end='')
        first = True
        for key, val in self.op_keys.items():
            if not val in job.keys():
                continue
            if not first:
                print(", ", end='')

            opname = key
            if self.args.fullname:
                opname = self.op_keys[key]

            print('%s: %d' % (opname, job[val]), end='')
            if first:
                first = False
        print('}')

    def print_top_jobs(self, top_jobs, total_jobs, count, timevalue):
        '''
        print top_jobs in YAML
        '''
        global RATE
        global HOSTS

        print('---') # mark the begining of YAML doc in stream
        print(f'timestamp: {int(time.time())}')
        if RATE:
            print(f'sample_duration: {timevalue}')
        print(f'servers_queried: {len(HOSTS)}')
        print(f'total_jobs: {total_jobs}')
        print(f'top_{count}_jobs:')
        for job in top_jobs:
            self.print_job(job)
        print('...') # mark the end of YAML doc in stream

    def run_once_ser(self, HOSTS, STATSPARAM, SSHUSER, SSHKEY, SSHKEYTYPE, TYPE, HOSTPARAM):
        '''
        scan/parse/aggregate/print top jobs in given job_stats pattern/path(s)
        '''
        global RATE
        global REFERENCE
        global REFERENCE_TIME
        jobs = {}

        QUERY_TIME = int(time.time())
        STATSDATA = self.GetData(HOSTS, STATSPARAM, SSHUSER, SSHKEY, SSHKEYTYPE, TYPE, HOSTPARAM)

        for data in STATSDATA:
            obj = self.parse_single_job_stats(data)
            if obj['job_stats'] is None:
                continue

            for job in obj['job_stats']:
                self.merge_job(jobs, job)

        total_jobs = len(jobs)
        if RATE and not REFERENCE:
            jobs, duration = self.RateCalc(jobs, QUERY_TIME)
        elif RATE and REFERENCE:
            jobs, duration = self.RateCalc(jobs, QUERY_TIME)
            top_jobs = self.pick_top_jobs(jobs, self.args.count)
            self.print_top_jobs(top_jobs, total_jobs, self.args.count, duration)
        else:
            top_jobs = self.pick_top_jobs(jobs, self.args.count)
            self.print_top_jobs(top_jobs, total_jobs, self.args.count, QUERY_TIME)
        

    def run_once_par(self, HOSTS, STATSPARAM, SSHUSER, SSHKEY, SSHKEYTYPE, TYPE, HOSTPARAM):
        '''
        scan/parse/aggregate/print top jobs in given job_stats pattern/path(s)
        '''
        global RATE
        global REFERENCE
        global REFERENCE_TIME
        jobs = {}

        QUERY_TIME = int(time.time())
        STATSDATA = self.GetData(HOSTS, STATSPARAM, SSHUSER, SSHKEY, SSHKEYTYPE, TYPE, HOSTPARAM)

        objs = []
        procs = []
        Q = Queue()
        
        try:
            for data in STATSDATA:
                p = Process(target=self.parse_single_job_stats, args=(Q, [data]))
                procs.append(p)
                p.start()
            for p in procs:
                r = Q.get() # blocking
                objs.append(r)
            for p in procs:
                p.join()
        except Exception as e:
            print(e)
            sys.exit()

        for obj in objs:
            if obj['job_stats'] is None:
                continue

            for job in obj['job_stats']:
                self.merge_job(jobs, job)

        total_jobs = len(jobs)
        if RATE and not REFERENCE:
            jobs, duration = self.RateCalc(jobs, QUERY_TIME)
        elif RATE and REFERENCE:
            jobs, duration = self.RateCalc(jobs, QUERY_TIME)
            top_jobs = self.pick_top_jobs(jobs, self.args.count)
            self.print_top_jobs(top_jobs, total_jobs, self.args.count, duration)
        else:
            top_jobs = self.pick_top_jobs(jobs, self.args.count)
            self.print_top_jobs(top_jobs, total_jobs, self.args.count, QUERY_TIME)

    def run_once_retry(self, HOSTS, STATSPARAM, SSHUSER, SSHKEY, SSHKEYTYPE, TYPE, HOSTPARAM):
        '''
        Call run_once. If run_once succeeds, return.
        If run_once throws an exception, retry for few times.
        '''
        for i in range(2, -1, -1):  # 2, 1, 0
            try:
                return self.run_once_par(HOSTS, STATSPARAM, SSHUSER, SSHKEY, SSHKEYTYPE, TYPE, HOSTPARAM)
                #return self.run_once_ser(HOSTS, STATSPARAM, SSHUSER, SSHKEY, SSHKEYTYPE, TYPE, HOSTPARAM)
            except Exception as e: # pylint: disable=bare-except
                if i == 0:
                    raise


    def SSHGet(self, queue, HOST, STATSPARAM, SSHUSER, SSHKEY, SSHKEYTYPE, TYPE, CMD):
        try:

            if SSHKEYTYPE == "DSS" or SSHKEYTYPE == "DSA":
                ssh_pkey = paramiko.DSSKey.from_private_key_file(filename=SSHKEY)

            if SSHKEYTYPE == "ECDSA":
                ssh_pkey = paramiko.ECDSAKey.from_private_key_file(filename=SSHKEY)

            if SSHKEYTYPE == "RSA":
                ssh_pkey = paramiko.RSAKey.from_private_key_file(filename=SSHKEY)

            if SSHKEYTYPE == "Ed25519":
                ssh_pkey = paramiko.Ed25519Key.from_private_key_file(filename=SSHKEY)

            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(hostname=HOST, username=SSHUSER, pkey=ssh_pkey)

            try:
                stdin, stdout, stderr = ssh.exec_command(CMD)
            except Exeption as e:
                ssh.close()
                print(e)
                error = stderr.read().decode(encoding='UTF-8')
                print(error)
                return "Exeption running ssh.exec_command"
            else:
                output = stdout.read().decode(encoding='UTF-8')

            if TYPE == "param":
                hostparam = {HOST: output.split()}
                queue.put(hostparam)
            if TYPE == "stats":
                queue.put(output)

        except KeyboardInterrupt as e:
            print('Received KeyboardInterrupt in run()')
            sys.exit()

    def GetData(self, HOSTS, STATSPARAM, SSHUSER, SSHKEY, SSHKEYTYPE, TYPE, HOSTPARAM):
        '''
        comment
        '''
        if TYPE == "param":
            HOSTDATA = {}
        if TYPE == "stats":
            HOSTDATA = []

        procs = []
        Q = Queue()

        try:
            for HOST in HOSTS:
                if TYPE == "param":
                    CMD = f'lctl list_param {STATSPARAM}'
                    p = Process(target=self.SSHGet, args=(Q, HOST, STATSPARAM, SSHUSER, SSHKEY, SSHKEYTYPE, TYPE, CMD))
                    procs.append(p)
                    p.start()
                if TYPE == "stats":
                    for PARAM in HOSTPARAM[HOST]:
                        CMD = f'lctl get_param -n {PARAM}'
                        p = Process(target=self.SSHGet, args=(Q, HOST, STATSPARAM, SSHUSER, SSHKEY, SSHKEYTYPE, TYPE, CMD))
                        procs.append(p)
                        p.start()
 
            for p in procs:
                r = Q.get() # blocking
                if TYPE == "param":
                    HOSTDATA.update(r)
                if TYPE == "stats":
                    HOSTDATA.append(r)
            for p in procs:
                p.join()
        except Exception as e:
            print(e)
            sys.exit()            
        else:
            return HOSTDATA

    def RunBEO(self):
        '''
        run task periodically or for some times with given interval
        '''
        argparser = ArgParser()
        argparser.run()
        self.args = argparser.args

        STATSPARAM = self.args.param
        HOSTS = argparser.serverlist
        SSHUSER = argparser.user
        SSHKEY = argparser.key
        SSHKEYTYPE = argparser.keytype
        
        CMD = f'lctl list_param {STATSPARAM}'
        HOSTPARAM = self.GetData(HOSTS, STATSPARAM, SSHUSER, SSHKEY, SSHKEYTYPE, "param", "")
        
        i = 0
        try:
            while True:
                self.run_once_retry(HOSTS, STATSPARAM, SSHUSER, SSHKEY, SSHKEYTYPE, "stats", HOSTPARAM)
                i += 1
                if self.args.repeats != -1 and i >= self.args.repeats:
                    break
                time.sleep(self.args.interval)
        except (KeyboardInterrupt):
            print("\nReceived KeyboardInterrupt - stopping")
            sys.exit()
 
if __name__ == "__main__":
    JobStatsParser().RunBEO()
       
