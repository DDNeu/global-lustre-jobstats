#!/usr/bin/env python3
#
#
'''
Suggestion from https://stackoverflow.com/a/77033777/15349369
'''

import multiprocessing
import time

num_p = 2
num_h = 10
num_s = 1


def worker(index):
    """
    Do some fake work
    """
    print(f"SSH to host {index} and grab some data")
    tw_start = time.time()
    fake_work_time = num_s
    time.sleep(fake_work_time)
    tw_stop = time.time()
    tw_elapsed = tw_stop - tw_start
    result = [index + 1000, tw_elapsed]
    print(f"Result from host {index} is {result[0]} and was returned after {tw_elapsed} seconds")
    return (index, result)


def run_par():
    work = range(num_h)
    with multiprocessing.Pool(processes=num_p) as p:
        return dict(p.imap_unordered(worker, work))


if __name__ == "__main__":
    print(f"Parallel processes: {num_p}")
    print(f"Hosts to query: {num_h}")
    print(f"Fake work time (s): {num_s}")
    if num_h < num_p:
        est_runtime = round(num_s * num_h / num_h, 1)
    else:
        est_runtime = round(num_s * num_h / num_p, 1)
    print(f"Expected runtime (s): {est_runtime}\n")

    tm_start = time.time()
    results = run_par()
    tm_stop = time.time()

    tm_elapsed = tm_stop - tm_start
    print("\nResults:\n", results)
    print(f"\nActual runtime: {tm_elapsed}")
