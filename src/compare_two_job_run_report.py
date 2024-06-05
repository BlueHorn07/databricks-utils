import os
import argparse
import pandas as pd
import scipy.stats as stats


from .DatabricksApi import DatabricksApi

host_url = os.environ.get('DATABRICKS_HOST_URL')
token = os.environ.get('DATABRICKS_TOKEN')

api = DatabricksApi(access_token=token, host_url=host_url)

def get_job_run_stats(job_id: str | int, num_job_runs: int = 10):
    print(f'Get job runs for job_id: {job_id}')
    job_runs = api.list_job_runs(job_id=job_id, completed_only=True, limit=num_job_runs)

    df = pd.DataFrame(job_runs['runs'])

    return df['run_duration'].to_list()


def get_basic_stats(duration_list: list):
    avg_duration = sum(duration_list) / len(duration_list)
    max_duration = max(duration_list)
    min_duration = min(duration_list)

    return (int(avg_duration // 1000), int(max_duration // 1000), int(min_duration // 1000))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--job-id-1', type=str, required=True)
    parser.add_argument('--job-id-2', type=str, required=True)
    parser.add_argument('--num-job-runs', type=str, required=False, default=10)
    parser.add_argument('--run-statistical-test', type=str, required=False, default=False)
    args = parser.parse_args()

    job1 = api.get_single_job(args.job_id_1)
    job1_name = job1['settings']['name']
    job1_run_list = get_job_run_stats(args.job_id_1, args.num_job_runs)
    stats1 = get_basic_stats(job1_run_list)
    
    job2 = api.get_single_job(args.job_id_2)
    job2_name = job2['settings']['name']
    job2_run_list = get_job_run_stats(args.job_id_2, args.num_job_runs)
    stats2 = get_basic_stats(job2_run_list)

    print()

    print("===== REPORT =====")
    max_name_len = max(len(job1_name), len(job2_name))
    max_id_len = max(len(args.job_id_1), len(args.job_id_2))
    print(f'"{job1_name.ljust(max_name_len)}" ({args.job_id_1.ljust(max_id_len)}): (Avg, Max, Min) = {stats1} Seconds')
    print(f'"{job2_name.ljust(max_name_len)}" ({args.job_id_2.ljust(max_id_len)}): (Avg, Max, Min) = {stats2} Seconds')

    if args.run_statistical_test:
        t_stat, p_value = stats.ttest_ind(job1_run_list, job2_run_list)
        alpha = 0.05
        print(f"p-value={p_value}")
        if p_value < alpha:
            print("There is a significant time difference between the two jobs.")
        else:
            print("There is no significant time difference between the two jobs.")
    print("==================")
