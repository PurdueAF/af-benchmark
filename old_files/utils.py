import sys
import glob
from functools import partial
import time
import math
import tqdm
import numpy as np
import pandas as pd
from coffea.nanoevents import NanoEventsFactory, NanoAODSchema
from memory_profiler import memory_usage


def get_list_of_files(datasets):
    prefix = "root://eos.cms.rcac.purdue.edu/"
    prefix_mount = "/eos/purdue/"
    files = []
    for dataset in datasets:
        ds_files = glob.glob(prefix_mount+dataset+"/*/*root")
        print(f"{dataset}: {len(ds_files)}")
        files.extend(ds_files)
    # replace explicit path /eos/purdue with XRootD prefix
    files = [f.replace(prefix_mount, prefix) for f in files]
    return files


def get_columns(file, **kwargs):
    '''
        Structure of a NanoAOD file looks like this:
          - branch1
            - leaf1
            - leaf2
          - branch2
            - leaf1
            - leaf2
            - leaf3
        We will return list of the *names* of branches/leaves
        in the following format:
          [
            [branch1, leaf1],
            [branch1, leaf2],
            [branch2, leaf1],
            [branch2, leaf2],
            [branch2, leaf3],
          ]
    '''

    fraction = kwargs.get("fraction", None)
    n_columns = kwargs.get("n_columns", None)
    if (fraction is not None) and (n_columns is not None):
        raise Exception("Only one of (`n_columns`, `fraction`) must be specified!")
        
    # read NanoAOD event
    events = NanoEventsFactory.from_root(
        file,
        schemaclass=NanoAODSchema.v6,
        uproot_options={"timeout": 120}
    ).events()

    all_columns = np.empty((0, 2), dtype=float)

    # loop over branches
    for branch in events.fields:
        # loop over leaves
        for leaf in events[branch].fields:
            # save names of branches and leaves into a flat array
            all_columns = np.append(all_columns, [np.array([branch, leaf])], axis=0)

    if n_columns is not None:
        # select a given number of columns
        if n_columns < 0:
            print("Number of columns cannot be lower than 0! Resetting to 0.")
            n_columns = 0
        elif n_columns > len(all_columns):
            print(
                f"Requested number of columns ({n_columns}) exceeds total "
                f"number of leaves in file ({len(all_columns)})."
                f"Will return only {len(all_columns)} columns."
            )
            n_columns = len(all_columns)
        columns_subset = all_columns[:n_columns]
    elif fraction is not None:
        # select a fraction of leaves
        if fraction > 1:
            print("Fraction cannot be greater than 1! Resetting to 1.")
            fraction = 1
        if fraction < 0:
            print("Fraction cannot be lower than 0! Resetting to 0.")
            fraction = 0
        columns_subset = all_columns[:math.ceil(fraction * len(all_columns))]
    else:
        # default: select all columns
        columns_subset = all_columns

    return columns_subset

def process(file, columns=[], measure_mem=False):
    '''
    Workflow to run for each file
    '''
    events = NanoEventsFactory.from_root(
        file,
        schemaclass=NanoAODSchema.v6,
        uproot_options={"timeout": 120}
    ).events()

    # We will compute mean values for a given subset of columns.
    # This way we can be sure that we access every element in a column.
    mean_values = {}
    mem_usage_mb = {}
    for column in columns:
        branch = column[0]
        leaf = column[1]
        mem_usage_mb[f"{branch}_{leaf}"] = 0
        mean_values[f"{branch}_{leaf}"] = 0
        if leaf in events[branch].fields:
            if measure_mem:
                mem_before = memory_usage()[0]
            mean_values[f"{branch}_{leaf}"] = np.mean(events[branch][leaf])
            if measure_mem:
                mem_usage_mb[f"{branch}_{leaf}"] = memory_usage()[0] - mem_before                            
    nevents = len(events)
    if measure_mem:
        mem_usage_mb = pd.DataFrame([mem_usage_mb])
    return nevents, mean_values, mem_usage_mb


def run_benchmark(process, files, columns=[], parallel=False, client=None, measure_mem=False):
    '''
    Measure time for a list of files
    '''
    tick = time.time()

    nevts_total = 0
    columns_ = [f"{c[0]}_{c[1]}" for c in columns]
    mem_usage_full = pd.DataFrame(columns=sorted(columns_))

    if parallel:
        # Parallel processing using Dask
        if not client:
            raise "Dask client is missing!"
        futures = client.map(partial(process, columns=columns, measure_mem=measure_mem), files)
        results = client.gather(futures)
        for r in results:
            nevts, mean_vals, mem_usg_mb = r
            nevts_total += nevts
            if measure_mem:
                mem_usage_full = pd.concat([mem_usage_full, mem_usg_mb])
    else:
        # Sequential processing
        for file in tqdm.tqdm(files):
            nevts, mean_vals, mem_usg_mb = process(file, columns=columns, measure_mem=measure_mem)
            nevts_total += nevts
            if measure_mem:
                mem_usage_full = pd.concat([mem_usage_full, mem_usg_mb])

    tock = time.time()
    elapsed = tock - tick

    print(nevts_total, "events")
    print(round(elapsed,3), "s")
    print(nevts_total/elapsed, "evts/s")
    if measure_mem:
        # print(mem_usage_full)
        # print(mem_usage_full.sum())
        print(mem_usage_full.sum().sum())
        with open("output.txt", 'w') as file:
            file.write(str(mem_usage_full.sum().sum()))
