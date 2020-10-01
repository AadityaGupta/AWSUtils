import json
import urllib.parse
import boto3
import botocore
import tempfile
import os
import io
import re
import datetime

import email.mime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication 

print('Loading function')

s3 = boto3.client('s3')
temp_file = tempfile.mktemp()

def lambda_handler(event, context):
   #destination_bucket_name = 'destination-test-bucket-006'

   # event contains all information about uploaded object
   print("Event :", event)

   # Bucket Name where file was uploaded
   source_bucket_name = event['Records'][0]['s3']['bucket']['name']

   # Filename of object (with path)
   file_key_name = event['Records'][0]['s3']['object']['key']
   
   split_key = re.split('/', file_key_name)
   print(split_key[-1])
   
   email_subject=set_email_subject(source_bucket_name,split_key) 

   # Copy Source Object
   copy_source_object = {'Bucket': source_bucket_name, 'Key': file_key_name}
   
   send_email('<enter sender email id>', '<enter recipient email id>', 'us-east-1', email_subject, source_bucket_name, file_key_name, split_key[-1])

   # S3 copy object operation
   #s3_client.copy_object(CopySource=copy_source_object, Bucket=destination_bucket_name, Key=file_key_name)

   return {
       'statusCode': 200,
       'body': json.dumps('Hello from S3 events Lambda!')
   }

def send_email(sender, recipient, aws_region, subject, bucket_name,key_name,split_key_name):
    ''' The email body for recipients with non-HTML email clients.
    The HTML body of the email.'''
    
    BODY_TEXT = "Hello,\r\nPlease find the attached file."     
    BODY_HTML = '''\
    <html>
    <head></head>
    <body>
    <h3>Hello!</h3>
    <p>Please find attached the ''' + split_key_name + ''' file.</p>
    </body>
    </html>
    '''    
    CHARSET = "utf-8"
    ses_client = boto3.client('ses',region_name=aws_region)    
    msg = MIMEMultipart('mixed')
    ''' Add subject, from and to lines.'''
    msg['Subject'] = subject 
    msg['From'] = sender 
    msg['To'] = recipient
    msg['Cc'] = sender  
    msg_body = MIMEMultipart('alternative')
    
    textpart = MIMEText(BODY_TEXT.encode(CHARSET), 'plain', CHARSET)
    htmlpart = MIMEText(BODY_HTML.encode(CHARSET), 'html', CHARSET)    
    '''Add the text and HTML parts to the child container.'''
    
    msg_body.attach(textpart)
    msg_body.attach(htmlpart)    
    '''Define the attachment part and encode it using MIMEApplication.'''
    s3.download_file(bucket_name, key_name,temp_file)
    ATTACHMENT=temp_file
    att = MIMEApplication(open(ATTACHMENT, 'rb').read())    
    att.add_header('Content-Disposition','attachment',filename=split_key_name) 
    
    if os.path.exists(temp_file):
        print("File exists")
    else:
        print("File does not exists")    
        
    '''Attach the multipart/alternative child container to the multipart/mixed
    # parent container.'''
    msg.attach(msg_body)    
    '''Add the attachment to the parent container.'''
    msg.attach(att)
    print("msg and body attached")
    
    try:
        '''Provide the contents of the email.'''
        response = ses_client.send_raw_email(
            Source=msg['From'],
            Destinations=[
                msg['To'], msg['Cc']
            ],
            RawMessage={
                'Data':msg.as_string(),
            }
        )   
        print ("client sent mail")
        '''Display an error if something goes wrong. '''
    except botocore.exceptions.ClientError as e:
        print ("error occurred")
        print (e)
        return(e.response['Error']['Message'])
    else:
        print ("message sent")
        return("Email sent! Message ID:", response['MessageId'])
        
        
    
    
def set_email_subject(str_source_bucket_name,arr_split_key):
    
    emailsubject= str_source_bucket_name
    extract_str = 'file'
    now = datetime.datetime.now()
    
    for str in arr_split_key:
        if str.lower() == '<enter string name>'.lower():
            extract_str= '<subject string>'
            
    emailsubject = extract_str + ' on ' + now.strftime("%d-%b-%Y") + ' | attached file ' + arr_split_key[-1]
    
    return emailsubject
    