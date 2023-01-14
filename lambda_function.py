import boto3
import io
import re
import time
import csv
from datetime import datetime

params = {
    'region': 'us-east-1',
    'database': 'database',
    'bucket': 'rafawainer-athena-account-s3-lambda-extract',
    'path': 'temp/athena/output',
    'query': 'SELECT account_id as account_id, name as name, date_of_birth as date_of_birth, status as status, expires_at as expires_at FROM account_s3 LIMIT 100'
}

temp_file_location = "/tmp/athena_query_results.csv"

session = boto3.Session()
athena_client = session.client('athena', region_name=params['region'])


##################################
##### A T H E N A  Q U E R Y
##################################

def athena_query(athena_client, params):
    response = athena_client.start_query_execution(
        QueryString=params["query"],
        QueryExecutionContext={
            'Database': params['database']
        },
        ResultConfiguration={
            'OutputLocation': 's3://' + params['bucket'] + '/' + params['path']
        }
    )
    return response


##################################
##### A T H E N A  Q U E R Y  R E S U L T S
##################################
def athena_query_results(athena_client, execution_id):
    while True:
        try:
            print("Iniciando tentativa de response da queryExecutionId == " + execution_id)
            query_results = athena_client.get_query_results(
                QueryExecutionId=execution_id,
                MaxResults=1000
            )
            print("Iniciando for result in query_results " + query_results)
            for result in query_results:
                print("Response: {}".format(result))
            break
        except Exception as err:
            if "Query has not yet finished" in str(err):
                time.sleep(0.1)
        else:
            raise err


##################################
##### A T H E N A  T O  S 3
##################################
def athena_to_s3(athena_client, params, max_execution=10):
    execution = athena_query(athena_client, params)
    execution_id = execution['QueryExecutionId']
    state = 'RUNNING'

    while (max_execution > 0 and state in ['RUNNING', 'QUEUED']):
        max_execution = max_execution - 1

        response = athena_client.get_query_execution(QueryExecutionId=execution_id)

        if 'QueryExecution' in response and \
                'Status' in response['QueryExecution'] and \
                'State' in response['QueryExecution']['Status']:
            state = response['QueryExecution']['Status']['State']

            if state == 'FAILED':
                return False
            elif state == 'SUCCEEDED':

                query_results = athena_client.get_query_results(
                    QueryExecutionId=execution_id,
                    MaxResults=1000
                )

                query_results_data = query_results['ResultSet']['Rows']

                row = 0
                rows = []
                print("Iniciando for result in query_results_data ")
                for result in query_results_data:
                    if row == 0:
                        header_account_id = result['Data'][0]['VarCharValue']
                        header_name = result['Data'][1]['VarCharValue']
                        header_date_of_birth = result['Data'][2]['VarCharValue']
                        header_status = result['Data'][3]['VarCharValue']
                        header_expires_at = result['Data'][4]['VarCharValue']
                        print("Cabecalho => {}, {}, {}, {}, {}".format(header_account_id, header_name,
                                                                       header_date_of_birth, header_status,
                                                                       header_expires_at))
                    else:
                        detail_account_id = result['Data'][0]['VarCharValue']
                        detail_name = result['Data'][1]['VarCharValue']
                        detail_date_of_birth = result['Data'][2]['VarCharValue']
                        detail_status = result['Data'][3]['VarCharValue']
                        detail_expires_at = result['Data'][4]['VarCharValue']
                        print("Response ==> {}: {}, {}: {}, {}: {}, {}: {}, {}: {}".format(
                            header_account_id, detail_account_id,
                            header_name, detail_name,
                            header_date_of_birth, detail_date_of_birth,
                            header_status, detail_status,
                            header_expires_at, detail_expires_at))

                    account_id = result['Data'][0]['VarCharValue']
                    name = result['Data'][1]['VarCharValue']
                    date_of_birth = result['Data'][2]['VarCharValue']
                    status = result['Data'][3]['VarCharValue']
                    expires_at = result['Data'][4]['VarCharValue']
                    record = (account_id, name, date_of_birth, status, expires_at)
                    rows.append(record)
                    row += 1

                s3_path = response['QueryExecution']['ResultConfiguration']['OutputLocation']
                filename = re.findall('.*\/(.*)', s3_path)[0]

                # athena_query_results(athena_client, execution_id)

                return execution_id, rows
        time.sleep(1)

    return False


##################################
#####  W R I T E  C S V
##################################
def write_csv(temp_file_location, rows):
    print("Iniciando read_csv do arquivo {}".format(temp_file_location))
    output = open(temp_file_location, "w", encoding='utf-8-sig', newline='')
    registros = csv.writer(output)

    for linhas in rows:
        print(linhas)
        registros.writerow(linhas)

    print("Finalizado write_csv do arquivo {}".format(temp_file_location))
    output.close()


##################################
##### S A V E  S 3  F I L E S
##################################
def s3_save_file(session, params, temp_file_location):
    s3_client = boto3.client('s3')
    print("Iniciando a cópia do arquivo")
    df = pd.read_csv(read_file['Body'])

    mem_file = io.BytesIO()
    s3_client.put_object(Bucket=params['bucket'], Key="python/accounts_csv-{}".format(str(datetime.now())),
                         Body=mem_file)
    print("Finalizado o download do arquivo {temp_file_location}")

    return temp_file_location


##################################
##### D E L E T E  S 3  F I L E S
##################################
# Deletes all files in your path so use carefully!
def cleanup(session, params):
    s3 = session.resource('s3')
    my_bucket = s3.Bucket(params['bucket'])
    for item in my_bucket.objects.filter(Prefix=params['path']):
        item.delete()


##################################
##### M A I N --> L A M B D A  H A N D L E R
##################################
def lambda_handler(event, context):
    # Query Athena and get the s3 filename as a result
    s3_filename = athena_to_s3(athena_client, params)

    # Work with S3 csv file
    rows = s3_filename[1]
    write_csv_file = write_csv(temp_file_location, rows)

    # Save CSV file generated into S3 Bucket
    # save_csv_into_s3 = s3_save_file(session,params,temp_file_location)

    # Removes all files from the s3 folder you specified, so be careful
    # cleanup(session, params)