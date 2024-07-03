import os
import argparse
import pandas as pd
import scipy.stats as stats

from .utils.DatabricksApi import DatabricksApi

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

    pandas_df = pd.DataFrame(
        columns=['Job Name', 'Job ID', 'Num Job Runs', 'Avg (sec)', 'Max (sec)', 'Min (sec)'],
        data={
            'Job Name': [job1_name, job2_name],
            'Job ID': [args.job_id_1, args.job_id_2],
            'Num Job Runs': [len(job1_run_list), len(job2_run_list)],
            'Avg (sec)': [stats1[0], stats2[0]],
            'Max (sec)': [stats1[1], stats2[1]],
            'Min (sec)': [stats1[2], stats2[2]]
        })

    print("===== REPORT =====")
    print(pandas_df.to_markdown(tablefmt="simple_outline", index=False, colalign=('left', 'right')))

    if args.run_statistical_test:
        t_stat, p_value = stats.ttest_ind(job1_run_list, job2_run_list)
        alpha = 0.05
        print(f"p-value={p_value:.3f}")
        if p_value < alpha:
            print("*There is a **SIGNIFICANT TIME DIFFERENCE** between the two jobs.")
        else:
            print("*There is no significant time difference between the two jobs.")

    print("==================")
