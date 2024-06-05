import os
import argparse
import pandas as pd

from .DatabricksApi import DatabricksApi

host_url = os.environ.get('DATABRICKS_HOST_URL')
token = os.environ.get('DATABRICKS_TOKEN')

api = DatabricksApi(access_token=token, host_url=host_url)

def get_job_run_stats(job_id: str | int, num_job_runs: int = 10):
    print(f'Get job runs for job_id: {job_id}')
    job_runs = api.list_job_runs(job_id=job_id, completed_only=True, limit=num_job_runs)

    df = pd.DataFrame(job_runs['runs'])
    
    avg_duration = df['run_duration'].mean()
    max_duration = df['run_duration'].max()
    min_duration = df['run_duration'].min()

    return (int(avg_duration // 1000), int(max_duration // 1000), int(min_duration // 1000))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--job-id-1', type=str, required=True)
    parser.add_argument('--job-id-2', type=str, required=True)
    parser.add_argument('--num-job-runs', type=str, required=False, default=10)
    args = parser.parse_args()

    job1 = api.get_single_job(args.job_id_1)
    job1_name = job1['settings']['name']
    stats1 = get_job_run_stats(args.job_id_1, args.num_job_runs)
    
    job2 = api.get_single_job(args.job_id_2)
    job2_name = job2['settings']['name']
    stats2 = get_job_run_stats(args.job_id_2, args.num_job_runs)
    print()

    print("===== REPORT =====")
    max_name_len = max(len(job1_name), len(job2_name))
    max_id_len = max(len(args.job_id_1), len(args.job_id_2))
    print(f'"{job1_name.ljust(max_name_len)}" ({args.job_id_1.ljust(max_id_len)}): (Avg, Max, Min) = {stats1} Seconds')
    print(f'"{job2_name.ljust(max_name_len)}" ({args.job_id_2.ljust(max_id_len)}): (Avg, Max, Min) = {stats2} Seconds')
    print("==================")
