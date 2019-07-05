from __future__ import print_function
import os.path
import mimetypes
import base64
import boto3
import time
import csv
import json
import argparse
import sys

from datetime import datetime as dt
from dateutil.parser import parse

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request
from google.oauth2 import service_account

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

__version__ = "2.0" 

CREDENTIALS_FILE = 'credentials.json'
SCOPES = ['https://www.googleapis.com/auth/gmail.send']
EMAIL_FROM = ''
EMAIL_SUBJECT = 'Sales Query Results Attachment'
EMAIL_CONTENT = \
"""
    <html>
        <head></head>
        <body>
            <p style="color:black;"> Hello, <br>
                Please find your requested data attached. <br>
                It contains  {} per {} where {}. ({})     <br>
                Thank you. <br><br>
                
                Kind regards,<br>
                The Data Wizard
            </p>
        </body>
    </html>
"""


def create_message_with_attachment(
    sender, to, subject, message_text, file):
  """Create a message for an email.

  Args:
    sender: Email address of the sender.
    to: Email address of the receiver.
    subject: The subject of the email message.
    message_text: The text of the email message.
    file: The path to the file to be attached.

  Returns:
    An object containing a base64url encoded email object.
  """
  message = MIMEMultipart()
  message['to'] = to
  message['from'] = sender
  message['subject'] = subject

  msg = MIMEText(message_text, 'html')
  message.attach(msg)

  content_type, encoding = mimetypes.guess_type(file)

  if content_type is None or encoding is not None:
    content_type = 'application/octet-stream'
    
  main_type, sub_type = content_type.split('/', 1)
  
  if main_type == 'text':
    fp = open(file, 'rb')
    msg = MIMEText(fp.read(), _subtype=sub_type)
    fp.close()
    
  filename = os.path.basename(file)
  msg.add_header('Content-Disposition', 'attachment', filename=filename)
  message.attach(msg)

  return {'raw': base64.urlsafe_b64encode(message.as_string())}

def send_message(service, user_id, message):
  """Send an email message.

  Args:
    service: Authorized Gmail API service instance.
    user_id: User's email address. The special value "me"
    can be used to indicate the authenticated user.
    message: Message to be sent.

  Returns:
    Sent Message.
  """
  try:
    message = (service.users().messages().send(userId=user_id, body=message)
               .execute())
    print('Message Id: %s' % message['id'])
    return message
  except HttpError, error:
     print('An error occurred: %s' % error)
    
    
def fetch_results(request_data):  
    try:
        response = client.invoke(
        FunctionName='',
        InvocationType='RequestResponse',
        LogType='Tail',
        Payload=json.dumps(request_data)
        )
        
        res_json = json.loads(response['Payload'].read().decode("utf-8"))
    
        results = res_json.get('data', [])
        
        if debug: 
            print('results : {}'.format(results))
            print(res_json.keys())
            print("next token: {}".format(res_json.get('nextToken')))
            print("Writing {} results to csv".format(len(results)))
            
        write_to_csv(results)
        
        if is_valid_token(res_json.get('nextToken')):
            next_page_data = dict()
            next_page_data['keys'] = res_json.get('keys')
            next_page_data['queryExecutionId'] = res_json.get('queryExecutionId')
            next_page_data['nextToken'] = res_json.get('nextToken')
            fetch_results(next_page_data)
    except Exception:
        
        if debug:
            raise
        else:
            return False

    return True
    
def is_valid_token(token):
    return token not in [None, str()]

    
def write_to_csv(data):
    with open('/tmp/{}.csv'.format(FILE_ATTACHMENT), 'a') as output_file:
        dict_writer = csv.DictWriter(output_file, DIMENSIONS+MEASURES)
        dict_writer.writerows(data)    
    
def write_csv_headers():
    with open('/tmp/{}.csv'.format(FILE_ATTACHMENT), 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, DIMENSIONS+MEASURES)
        dict_writer.writeheader()
    
def lambda_handler(event, context):

    global client, FILE_ATTACHMENT, write_header, DIMENSIONS, MEASURES, PERIOD, FILTERS, debug
    
    EMAIL_TO = event.get("email")
    FILE_ATTACHMENT = event.get("file")
    DIMENSIONS = event.get("dimension")
    MEASURES = event.get("measure")
    PERIOD = event.get("period")
    FILTERS = event.get('filter')
    QUERY_EXECUTION_ID = event.get('queryExecutionId')
    event['keys'] = DIMENSIONS + MEASURES
    
    print("Query ID :{}".format(event.get('queryExecutionId')))
    
    debug = True
    write_header = True
    
    client =  boto3.client('lambda')
    athena_client =  boto3.client('athena')
   
    if debug: 
        print("Fetching query results")
        
    query_status = None
    
    while query_status == 'QUEUED' or query_status == 'RUNNING' or query_status is None:
        
        query_status = athena_client.get_query_execution(QueryExecutionId=QUERY_EXECUTION_ID)['QueryExecution']['Status']['State']
        
        if debug is True:
            print('Query Status: {} - {}'.format(query_status, dt.now().strftime("%H:%M:%S")))
            
        if query_status == 'FAILED' or query_status == 'CANCELLED':
            
            if debug is True:
                raise Exception('Athena query failed or was cancelled')
            else:
                return {
                    "status" : "Failed",
                    "statusCode": 500
                }
        time.sleep(5)
        
    # Write output file headers  
    write_csv_headers()
        
    # Fetch query results iteratively from fetch_results lambda and append to csv file
    status = fetch_results(event)
        
    if debug: 
        print("Sending email with query results") 

    # Send email
    if status is True: 
        credentials = service_account.Credentials.from_service_account_file(
        CREDENTIALS_FILE,
        scopes=SCOPES,
        subject=EMAIL_FROM)
        
        service = build('gmail', 'v1', credentials=credentials)
    
        # Call the Gmail API
        format_date = lambda date_string: parse(date_string).strftime('%Y/%m/%d')
        
        CONTENT = EMAIL_CONTENT.format(
                str(', ').join(['<b>{}</b>'.format(measure) for measure in MEASURES]),
                str(', ').join(['<b>{}</b>'.format(dim) for dim in DIMENSIONS]),
                str(", ").join(['<b>{}</b> is  '.format(key) + str(", ").join(values) for key, values in FILTERS.items()]),
                '{} - {}'.format(format_date(PERIOD.get("date_from")), format_date(PERIOD.get("date_to"))))
        
        email_attachment = create_message_with_attachment(
                EMAIL_FROM,
                EMAIL_TO,
                EMAIL_SUBJECT,
                CONTENT,
                '/temp/{}'.format(FILE_ATTACHMENT))
        
        send_message(service, user_id='me', message=email_attachment)
    
    return status

if __name__ == "__main__":
    # set arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--action", help="Gmail action to take i.e. Send, Read, Draft or Delete")
    parser.add_argument("--from-addr", help="Source Gmail address")
    parser.add_argument("--to-addr", help="Destination email address")
    parser.add_argument("--credentials", help="Google API Auth Credetials file")
    parser.add_argument("--email-subject", help="The email subject or heading")
    parser.add_argument("--email-message", help="The email message or path to file with the email message")
    parser.add_argument("--attachment", help="Text document to be sent with the email")
    args = parser.parse_args()
    
    # mandatory arguments
    if not args.action:
        print("Please enter a valid i.e. SEND, READ, DRAFT or DELETE ( --action argument )")
        sys.exit(1)
    if not args.from_addr:
        print("Please enter a valid Gmail address ( --from-addr argument )")
        sys.exit(1)
    if not args.to_addr:
        print("Please enter a valid email address ( --to-addr argument )")
        sys.exit(1)
    if not args.credentials:
        print("Please enter a valid path to Google API Auth Credentials ( --credentials argument )")
        sys.exit(1)
        
    # optional arguments
    if not args.email_subject:
        has_email_subject = False
    else:
        has_email_subject = True
    if not args.email_message:
        has_email_message = False
    else:
        has_email_message = True
    if not args.attachment:
        has_attachment = False
    else:
        has_attachment = True
        
    
        


