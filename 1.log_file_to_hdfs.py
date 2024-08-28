import os
from hdfs import InsecureClient


hdfs_client = InsecureClient('http://localhost:9870', user='kimminsu')

local_logs_path = '/Users/kimminsu/dmf/automation/logs/'
hdfs_logs_path = 'input/logs/'

local_files = os.listdir(local_logs_path)

for file_name in local_files:
    local_file_path = local_logs_path + file_name
    hdfs_file_path = hdfs_logs_path + file_name

    if hdfs_client.content(hdfs_file_path, strict=False):
        print(f'이미 {file_name}가 존재합니다.')
    else:
        hdfs_client.upload(hdfs_file_path, local_file_path)
        print(f'{file_name}를 업로드 했습니다.')
