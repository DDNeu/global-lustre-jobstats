#!/usr/bin/env python3
#
#
'''
Script to test how I can limit the number of parallel executions
'''

import sys
import time
#import random
import argparse
from multiprocessing import Process, Pool, Manager, cpu_count

parser = argparse.ArgumentParser(
                                prog='mp-test.py',
                                description='Try to limit number of parallel processes'
                                )

parser.add_argument('-nh', '--num-hosts', dest="num_hosts", type=int, default=10,
                    help='Number of hosts to ssh to (default 10).')
parser.add_argument('-np', '--num-proc', dest="num_proc", type=int, default=2,
                    help=f'Number of processes to spawn (default 2).')
parser.add_argument('-st', '--sleep-time', dest="sleep_time", type=int, default=10,
                    help='Time of imulated work in seconds (default 10).')

args = parser.parse_args()
num_p = args.num_proc
num_h = args.num_hosts
num_s = args.sleep_time

def worker(index, queue):
    '''
    Do some fake work
    '''
    print(f'SSH to host {index} and grab some data')
    tw_start = time.time()
    #fake_work_time = num_s + random.randrange(0, 10, 1)
    fake_work_time = num_s
    time.sleep(fake_work_time)
    tw_stop = time.time()
    tw_elapsed = tw_stop - tw_start
    result = [index + 1000, tw_elapsed]
    print(f'Result from host {index} is {result[0]} and was returned after {tw_elapsed} seconds')
    queue.put(result)

def reader(proc_q):
    '''
    Put the resuts of the fake work into Queue()
    '''
    message = proc_q.get()
    return message


def run_par():
    '''
    Run "worker" in parallel but limit the number of parallel executions
    to not overload the host running mp-test.py
    '''
    objs = {}
    procs = []
    mngr = Manager()
    proc_q = mngr.Queue()
    proc_p = Pool(processes=num_p)

    try:
        for host in range(num_h):
            proc = Process(target=worker, args=(host, proc_q))
            procs.append(proc)
            proc.start()

        readers = []
        for proc in procs:
            readers.append(proc_p.apply_async(reader, (proc_q,)))

        proc_p.close()
        proc_p.join()

        for enum, rdr in enumerate(readers):
            ret = rdr.get() # blocking
            objs.update({enum: ret})

    except Exception as exn:
        print("Something went wrong, doing something else instead")
        print(exn)
        sys.exit()

    return objs

if __name__ == "__main__":
    print(f'Parallel processes: {num_p}')
    print(f'Hosts to query: {num_h}')
    print(f'Fake work time (s): {num_s}')
    if num_h < num_p:
        est_runtime = round(num_s * num_h / num_h , 1)
    else:
        est_runtime = round(num_s * num_h / num_p , 1)
    print(f'Expected runtime (s): {est_runtime}\n')

    tm_start = time.time()
    results = run_par()
    tm_stop = time.time()

    tm_elapsed = tm_stop - tm_start
    print("\nResults:\n", results)
    print(f'\nActual runtime: {tm_elapsed}')
