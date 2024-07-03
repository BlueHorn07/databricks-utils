import os
import argparse

from .utils.DatabricksApi import DatabricksApi

host_url = os.environ.get('DATABRICKS_HOST_URL')
token = os.environ.get('DATABRICKS_TOKEN')

api = DatabricksApi(access_token=token, host_url=host_url)

def delete_notebook_by_pattern(path: str, pattern: str):
    print(f'Get notebooks for path: {path}')

    res = api.list_contents(path)

    print('Total notebooks:', len(res['objects']))

    deleted_cnt = 0

    for notebook in res['objects']:
        if pattern in notebook['path']:
            deleted_cnt += 1
            print(f'- [{deleted_cnt}] Delete notebook: {notebook["path"]}')
            api.delete_workspace_object(notebook['path'])

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', type=str, required=True)
    parser.add_argument('--pattern', type=str, required=True)
    args = parser.parse_args()

    delete_notebook_by_pattern(args.path, args.pattern)
