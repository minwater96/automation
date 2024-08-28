from pyhive import hive
import pandas as pd

conn = hive.Connection(
    host='localhost',
    port=10000,
    username='kimminsu',
)

cursor = conn.cursor()

query = '''
select ip_address, request_path from logs LIMIT 10
'''

# query = '''
# SELECT
#     ip_address,
#     COUNT(*),
# FROM
#     logs
# GROUP BY
#     ip_address
# ORDER BY
#     COUNT(*) DESC
# LIMIT 10
# '''

# query = '''
# SELECT
#     SPLIT(request_path, '/')[2] AS product_id,
#     COUNT(*) AS request_count
# FROM 
#     logs
# WHERE
#     request_path LIKE '/prorduct/%'
# GROUP BY
#     SPLIT(requset_path, '/')[2]
# ORDER BY
#     request_count DESC
# limit 10
# '''


cursor.execute(query)
result = cursor.fetchall()

df = pd.DataFrame(result)

output_path = '/Users/kimminsu/dmf/automation/logs-result/'
output_file = 'request_result.csv'

df.to_csv(output_path + output_file)