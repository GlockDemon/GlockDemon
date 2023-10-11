import boto3
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
import datetime
# How to use / Note
# Su dung aws access_key + secret access key cua tai khoan Pixta Vietnam
# aws configure
# nhap access_key + secret access key + aws region ap-north-east-1
# pip install google-api-python-client google-auth google-auth-httplib2 google-auth-oauthlib


### Khai bao thong tin credential voi AWS (ROLE)
role_arn = 'arn:aws:iam::567351176096:role/pixtavietnam-developers'

sts_client = boto3.client('sts')


### Su dung IAM role cua AWS
response = sts_client.assume_role(
    RoleArn=role_arn,
    RoleSessionName='AssumeRoleSessionName'
)

credentials = response['Credentials']

### Tao client voi IAM ROLE cua AWS
ce_client = boto3.client(
    'ce',
    region_name='ap-northeast-1',  # Change to the appropriate region
    aws_access_key_id=credentials['AccessKeyId'],
    aws_secret_access_key=credentials['SecretAccessKey'],
    aws_session_token=credentials['SessionToken']
)

### Services which we need
service_of_interest_list = ['Amazon DynamoDB','Amazon ElastiCache','Amazon Elastic Compute Cloud - Compute','Amazon Elastic Load Balancing','Amazon OpenSearch Service','Amazon Relational Database Service','Amazon Simple Storage Service', 'EC2 - Other'] 
# Tao vong lap kiem tra tung` tai khoan mot (one by one in 3)
# 04 Pixta VIETNAM, 25 PIXTA DEVELOPMENT, 80 PIXTA IMS
pixta_vietnam = '045675425505'      # pixta vietnam
pixta_development = '250506505253'  # pixta development
pixta_ims = '803585327025'          # pixta image service with product DAT

account_id = [pixta_vietnam,pixta_development,pixta_ims]

### Khai bao cac dictionary

value_dict_a = {}
value_dict_b = {}
value_dict_c = {}

### Define credential file
credentials = service_account.Credentials.from_service_account_file('/home/bachpham/Cost_explorer_aws/auto-update-cost-aws-pixta-vn-b2ceb51a16ae.json')

service = build('sheets', 'v4', credentials=credentials)

spreadsheet_id = '1a5kzlz2iOXaKKTMfCPr-xA1suZToptriUHitd98oKGc'
sheet = service.spreadsheets()


range_name = 'pixta_vn!D:N'

### Time in the query

today = datetime.date.today()

current_year = today.year

current_month =  today.month

previous_month = current_month - 1

current_month_str = today.strftime('%Y-%m')
previous_month_str = datetime.date(current_year, previous_month, 1).strftime('%Y-%m')

previous_month = current_month - 1

if previous_month == 0 :
    previous_month = 12

Start_time = f'{previous_month_str}-01'
End_time = f'{current_month_str}-01'

print(f'Updating cost on AWS for month number of:{previous_month} ')
print(Start_time)
print(End_time)

### Queries and Responses
for account_pointer in account_id:
    if account_pointer == pixta_ims:
        # C
        # Total cost of pixta ims
        query1 = {
            'TimePeriod': {
                'Start': Start_time,
                'End': End_time
            },
            'Granularity': 'MONTHLY',
            'Filter': {
                'And': [
                    {
                        'Dimensions': {
                            'Key': 'LINKED_ACCOUNT',
                            'Values': [account_pointer],
                        },
                    },
                    {
                        'Tags': {
                            'Key': 'Product',
                            'Values': ['DAT'],
                        },
                    },
                ],
            },
            'Metrics': ['UnblendedCost'] 
        }

        # Gui API request dau tien de lay total cost
        response1 = ce_client.get_cost_and_usage(**query1)

        # Lay Total cost cua tai khoan Pixta IMS (DAT)
        for result1 in response1['ResultsByTime']:
            total_cost_c = float(result1.get('Total', {}).get('UnblendedCost', {}).get('Amount', 0))
            print("Total cost of Pixta DAT")
            print(total_cost_c)
        query2 = {
            'TimePeriod': {
                'Start': Start_time,
                'End': End_time
            },
            'Granularity': 'MONTHLY',
            'Filter': {
                'And': [
                    {
                        'Dimensions': {
                            'Key': 'LINKED_ACCOUNT',
                            'Values': [account_pointer],
                        },
                    },
                    {
                        'Tags': {
                            'Key': 'Product',
                            'Values': ['DAT'],
                        },
                    },
                ],
            },
            'GroupBy': [
                {
                    'Type': 'DIMENSION',
                    'Key': 'SERVICE',
                },
            ],
            'Metrics': ['UnblendedCost']
        }

        response2 = ce_client.get_cost_and_usage(**query2)

        for service_of_interest in service_of_interest_list:
            for result2 in response2['ResultsByTime']:
                for group in result2.get('Groups', []):
                    keys = group.get('Keys', []) 
                    metrics = group.get('Metrics', [])

                    # gan data bang readable var
                    service_name = keys[0]

                    if service_name == service_of_interest:
                        cost = float(metrics.get('UnblendedCost', {}).get('Amount', 0))

                        value_dict_c[service_of_interest] = cost
        

    elif account_pointer == pixta_vietnam:
        # A
        #Total cost of pixta_vietnam
        query1 = {
        'TimePeriod': {
            'Start': Start_time,
            'End': End_time
        },
        'Granularity': 'MONTHLY',
        'Filter': {
            'Dimensions': {
                'Key': 'LINKED_ACCOUNT',
                'Values': [account_pointer],
            }
        },
        'Metrics': ['UnblendedCost']  
        }

        # Gui request de lay totalcost
        response1 = ce_client.get_cost_and_usage(**query1)
        
        # Parse Total cost of pixta_vietnam
        for result1 in response1['ResultsByTime']:
            total_cost_a = float(result1.get('Total', {}).get('UnblendedCost', {}).get('Amount', 0))        
            print("Total cost of pixta vietnam")
            print(total_cost_a)


        query2 = {
            'TimePeriod': {
                'Start': Start_time,
                'End': End_time
            },
            'Granularity': 'MONTHLY',
            'Filter': {
                'And': [
                            {
                                'Dimensions': {
                                    'Key': 'LINKED_ACCOUNT',
                                    'Values': [account_pointer] 
                                }
                            },
                            {
                                'Dimensions': {
                                    'Key': 'SERVICE',
                                    'Values': service_of_interest_list
                                }
                            }
                        ]
            },
            
            'GroupBy': [ 
                {
                    'Type': 'DIMENSION',
                    'Key': 'SERVICE'
                },
            ],
            'Metrics': ['UnblendedCost'] 
        }
        

        response2 = ce_client.get_cost_and_usage(**query2)

        for service_of_interest in service_of_interest_list:
            for result2 in response2['ResultsByTime']:
                for group in result2.get('Groups', []):
                    keys = group.get('Keys', []) 
                    metrics = group.get('Metrics', [])

                    # gan data bang readable var
                    service_name = keys[0]

                    if service_name == service_of_interest:
                        cost = float(metrics.get('UnblendedCost', {}).get('Amount', 0))

                        value_dict_a[service_of_interest] = cost
        

    elif account_pointer == pixta_development:
        # B
        # Total cost of pixta_development
        query1 = {
        'TimePeriod': {
            'Start': Start_time,
            'End': End_time
        },
        'Granularity': 'MONTHLY',
        'Filter': {
            'Dimensions': {
                'Key': 'LINKED_ACCOUNT',
                'Values': [account_pointer],
            }
        },
        'Metrics': ['UnblendedCost']  
        }

        # Gui request de lay totalcost
        response1 = ce_client.get_cost_and_usage(**query1)

        # Parse Total cost of pixta_development
        for result1 in response1['ResultsByTime']:
            total_cost_b = float(result1.get('Total', {}).get('UnblendedCost', {}).get('Amount', 0)) 
            print("Total cost of pixta-dev")
            print(total_cost_b)
        
        query2 = {
            'TimePeriod': {
                'Start': Start_time,
                'End': End_time
            },
            'Granularity': 'MONTHLY',
            'Filter': {
                'And': [
                            {
                                'Dimensions': {
                                    'Key': 'LINKED_ACCOUNT',
                                    'Values': [account_pointer] 
                                }
                            },
                            {
                                'Dimensions': {
                                    'Key': 'SERVICE',
                                    'Values': service_of_interest_list
                                }
                            }
                        ]
            },
            
            'GroupBy': [ 
                {
                    'Type': 'DIMENSION',
                    'Key': 'SERVICE'
                },
            ],
            'Metrics': ['UnblendedCost'] 
        }
        

        response2 = ce_client.get_cost_and_usage(**query2)
        
        for service_of_interest in service_of_interest_list:
            for result2 in response2['ResultsByTime']:
                for group in result2.get('Groups', []):
                    keys = group.get('Keys', []) 
                    metrics = group.get('Metrics', [])

                    # gan data bang readable var
                    service_name = keys[0]     

                    if service_name == service_of_interest:
                        cost = float(metrics.get('UnblendedCost', {}).get('Amount', 0))

                        value_dict_b[service_of_interest] = cost
                        # Ket thuc vong lap => ra ngoai


### Test if there is enough service returned. If not => 0

# For the dictionary B
for service in service_of_interest_list:
    if service in value_dict_b:
        exit
    else:
        value_dict_b[service] = 0


# For the dictionary A
for service in service_of_interest_list:
    if service in value_dict_a:
        exit
    else:
        value_dict_a[service] = 0


# For the dictionary C
for service in service_of_interest_list:
    if service in value_dict_c:
        exit
    else:
        value_dict_c[service] = 0


### Variables of all 3 accounts combined

print(value_dict_a)
print(value_dict_b)
print(value_dict_c)


S3 = value_dict_a['Amazon Simple Storage Service'] + value_dict_b['Amazon Simple Storage Service'] + value_dict_c['Amazon Simple Storage Service']
EC2 = value_dict_a['Amazon Elastic Compute Cloud - Compute'] + value_dict_b['Amazon Elastic Compute Cloud - Compute'] + value_dict_c['Amazon Elastic Compute Cloud - Compute']
ELB = value_dict_a['Amazon Elastic Load Balancing'] + value_dict_b['Amazon Elastic Load Balancing'] + value_dict_c['Amazon Elastic Load Balancing']
EC2_other = value_dict_a['EC2 - Other'] + value_dict_b['EC2 - Other'] + value_dict_c['EC2 - Other']
RDS = value_dict_a['Amazon Relational Database Service'] + value_dict_b['Amazon Relational Database Service'] + value_dict_c['Amazon Relational Database Service']
OpenSearch = value_dict_a['Amazon OpenSearch Service'] + value_dict_b['Amazon OpenSearch Service'] + value_dict_c['Amazon OpenSearch Service']
DynamoDB = value_dict_a['Amazon DynamoDB'] + value_dict_b['Amazon DynamoDB'] + value_dict_c['Amazon DynamoDB']
ECache = value_dict_a['Amazon ElastiCache'] + value_dict_b['Amazon ElastiCache'] + value_dict_c['Amazon ElastiCache']
EC2_total = EC2 + ELB + EC2_other
Total_cost = total_cost_a + total_cost_b + total_cost_c
Other = Total_cost - (S3 + EC2_total + RDS + OpenSearch + DynamoDB + ECache)

### Send values to Google Sheet file

# Khai bao bien array rong
values = []
values.append([S3,EC2_total,EC2,EC2_other,ELB,RDS,OpenSearch,DynamoDB,ECache,Other,Total_cost])

result = sheet.values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
existing_values = result.get('values', [])
existing_rows = len(existing_values)  if existing_values else 0
range_name_x = f'pixta_vn!D{existing_rows + len(values)}:N{existing_rows + len(values)}'
body =  {'values': values}
result = sheet.values().update(spreadsheetId=spreadsheet_id, range=range_name_x, valueInputOption='USER_ENTERED', body=body).execute()