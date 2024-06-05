import os
import argparse
import pandas as pd

from .DatabricksApi import DatabricksApi

host_url = os.environ.get('DATABRICKS_HOST_URL')
token = os.environ.get('DATABRICKS_TOKEN')

api = DatabricksApi(access_token=token, host_url=host_url)

def get_job_run_report(job_id: str | int, num_job_runs: int = 10):
    print(f'Get job runs for job_id: {job_id}')
    job_runs = api.list_job_runs(job_id=job_id, completed_only=True, limit=num_job_runs)

    df = pd.DataFrame(job_runs['runs'])
    
    avg_duration = df['run_duration'].mean()
    max_duration = df['run_duration'].max()
    min_duration = df['run_duration'].min()

    print(f'- Recent {len(df)} Job Run: (Avg, Max, Min) = ({int(avg_duration // 1000):,}, {int(max_duration // 1000):,}, {int(min_duration // 1000):,}) seconds')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--job-id', type=str, required=True)
    parser.add_argument('--num-job-runs', type=str, required=False, default=10)
    args = parser.parse_args()
    
    get_job_run_report(args.job_id, args.num_job_runs)
