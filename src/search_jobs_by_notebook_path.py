import os
import argparse
import pandas as pd
import time
import json

from .DatabricksApi import DatabricksApi

host_url = os.environ.get('DATABRICKS_HOST_URL')
token = os.environ.get('DATABRICKS_TOKEN')

cache_path = 'jobs.csv'

api = DatabricksApi(access_token=token, host_url=host_url)

def search_jobs_by_notebook_path(path: str, cache: bool):
    print(f'Get jobs for path: {path}')

    if cache and os.path.exists(cache_path):
        print(f'Load jobs from cache {cache_path}')
        df = pd.read_csv(cache_path)
    else:
        print('Get jobs from API')
        tic = time.time()
        jobs = api.list_jobs(expand_tasks=True)
        df = pd.DataFrame(jobs)
        print(f'Elapsed time: {time.time() - tic} seconds')

    if cache and not os.path.exists(cache_path):
        print(f'Cache jobs to {cache_path}')
        df.to_csv(cache_path, index=False)


    for idx, job in df.iterrows():
        settings = eval(job['settings'])
        tasks = settings['tasks']

        for task in tasks:
            if 'notebook_task' not in task:
                continue
            notebook_path = task['notebook_task']['notebook_path']

            if path in notebook_path:
                print(f'Job ID: {job["job_id"]:16d} Path: {notebook_path}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', type=str, required=True)
    parser.add_argument('--cache', type=bool, required=True)
    args = parser.parse_args()
    
    search_jobs_by_notebook_path(args.path, args.cache)
