'''
Created on 24 ago. 2018

@author: afunes
'''
import json
import logging
import requests
from requests.auth import HTTPBasicAuth


auth = HTTPBasicAuth("agustin22", "7dKNV4AiICFRnYyLwR2b")
headers = {'Plotly-Client-Platform': 'python'}

def get_pages(username, page_size):
    url = 'https://api.plot.ly/v2/folders/all?user='+username+'&page_size='+str(page_size)
    response = requests.get(url, auth=auth, headers=headers)
    if response.status_code != 200:
        return
    page = json.loads(response.content)
    yield page
    while True:
        resource = page['children']['next'] 
        if not resource:
            break
        response = requests.get(resource, auth=auth, headers=headers)
        if response.status_code != 200:
            break
        page = json.loads(response.content)
        yield page
        
def permanently_delete_files(username, page_size=500, filetype_to_delete='plot'):
    for page in get_pages(username, page_size):
        for x in range(0, len(page['children']['results'])):
            fid = page['children']['results'][x]['fid']
            res = requests.get('https://api.plot.ly/v2/files/' + fid, auth=auth, headers=headers)
            res.raise_for_status()
            if res.status_code == 200:
                json_res = json.loads(res.content)
                print(json_res['filetype'])
                if json_res['filetype'] == filetype_to_delete:
                    # move to trash
                    res1 = requests.post('https://api.plot.ly/v2/files/'+fid+'/trash', auth=auth, headers=headers)
                    if (res1.status_code == 200):
                        print ("successful moved to trash " + json_res['filetype'] + "-"+ page['children']['results'][x]['filename'])
                    else:
                        logging.warning("NOT successful moved to trash " + json_res['filetype'] + "-"+ page['children']['results'][x]['filename'])
                    # permanently delete
                    res2 = requests.delete('https://api.plot.ly/v2/files/'+fid+'/permanent_delete', auth=auth, headers=headers)
                    if (res2.status_code == 204):
                        print ("successful permanent delete " + json_res['filetype'] + "-"+ page['children']['results'][x]['filename'])
                    else:
                        logging.warning("NOT successful permanent delete " + json_res['filetype'] + "-"+ page['children']['results'][x]['filename'])

permanently_delete_files("agustin22", filetype_to_delete='plot')
permanently_delete_files("agustin22", filetype_to_delete='grid')