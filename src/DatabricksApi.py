import requests

# https://docs.databricks.com/api/workspace/introduction
class DatabricksApi():
    def __init__(self, access_token: str, host_url :str):
        self.host_url = host_url
        self.headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json',
        }

    def get_single_job(self, job_id: str | int):
        res = requests.get(f'{self.host_url}/api/2.1/jobs/get?job_id={job_id}', headers=self.headers)

        if res.status_code == 200:
            return res.json()
        else:
            raise Exception(res.text)
    
    def list_jobs(self, expand_tasks: bool = False):
        url = f'{self.host_url}/api/2.1/jobs/list'

        page_token = ''
        limit = 100
        page = 0

        jobs = []
        has_more = True
        while has_more:
            params = {'page_token': page_token, 'limit': limit}
            if expand_tasks:
                params['expand_tasks'] = 'true'
            else:
                params['expand_tasks'] = 'false'
            
            res = requests.get(url, params=params, headers=self.headers)
            jobs.extend(res.json()['jobs'])
            has_more = res.json()['has_more']
            if not has_more:
                break
            page_token = res.json()['next_page_token']
            page += 1
        return jobs

    def list_job_runs(self, job_id: str | int, active_only: bool = False, completed_only: bool = False, limit: int = 10, start_time_from: int = None, start_time_to: int = None):
        url = f'{self.host_url}/api/2.1/jobs/runs/list?job_id={job_id}'
        if active_only:
            url += '&active_only=true'
        if completed_only:
            url += '&completed_only=true'
        if limit:
            url += f'&limit={limit}'
        if start_time_from:
            url += f'&start_time_from={start_time_from}'
        if start_time_to:
            url += f'&start_time_to={start_time_to}'
        
        res = requests.get(url, headers=self.headers)

        if res.status_code == 200:
            return res.json()
        else:
            raise Exception(res.text)
