import os
import argparse

from .utils.DatabricksApi import DatabricksApi

host_url = os.environ.get('DATABRICKS_HOST_URL')
token = os.environ.get('DATABRICKS_TOKEN')

api = DatabricksApi(access_token=token, host_url=host_url)

def update_job_notebook_path(job_id: str | int, new_notebook_path: str):
    print(f'Get job for job_id: {job_id}')
    job = api.get_single_job(job_id=job_id)
    settings = job['settings']

    print('Job Name: ', settings['name'])

    if len(settings['tasks']) != 1:
        raise Exception('Only support job with single task')

    task = settings['tasks'][0]
    if 'notebook_task' not in task:
        raise Exception('Only support notebook task')

    notebook_path = task['notebook_task']['notebook_path']
    print(notebook_path)

    new_task = task.copy()
    new_task['notebook_task']['notebook_path'] = new_notebook_path

    new_settings = {
        'tasks': [new_task]
    }
    api.update_job_settings(job_id=job_id, settings=new_settings)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--job-id', type=str, required=True)
    parser.add_argument('--new-notebook-path', type=str, required=True)
    args = parser.parse_args()

    update_job_notebook_path(args.job_id, args.new_notebook_path)
