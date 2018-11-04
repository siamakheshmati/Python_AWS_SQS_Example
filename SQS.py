import boto3
import os
import logging
from datetime import datetime
import sys


_AWSACCESSKEY=""
_AWSSECRETACCESSKEY=""
QUEUE_URL=""
QUEUENAME=""
sqs=""
queue=""
client=""
unique_filename=""


#Transaction Time
_now="{:%Y-%m-%d %H:%M}".format(datetime.now())
#Python logger
logging.basicConfig(filename='log.log',level=logging.ERROR)

#We are going to use the two following environment variables to secure our script.
_AWSACCESSKEY=os.getenv("AWSACCESSKEY")
_AWSSECRETACCESSKEY=os.getenv("AWSSECRETACCESSKEY")

#Queue URL
QUEUE_URL='https://sqs.us-west-1.amazonaws.com/YOUR_AWS_ACCOUNT_NUMBER/YOUR_SQS_QUEUE_NAME'

#Queue name
QUEUENAME='YOUR_SQS_QUEUE_NAME'

sqs = boto3.resource('sqs', region_name='us-west-1',aws_access_key_id=_AWSACCESSKEY, aws_secret_access_key=_AWSSECRETACCESSKEY)
queue = sqs.get_queue_by_name(QueueName=QUEUENAME)

client = boto3.client('sqs', region_name='us-west-1',aws_access_key_id=_AWSACCESSKEY, aws_secret_access_key=_AWSSECRETACCESSKEY)
unique_filename = ""


#Writes a message to the queue
def writeMessage(incomingMessage):
    response= queue.send_message(QueueUrl=QUEUE_URL,DelaySeconds=0,MessageBody=(incomingMessage))
    print("MessageId created: {0}".format(response.get('MessageId') + " " + incomingMessage))



#Reads a message from the queue
def readMessage():
        messages = queue.receive_messages()
        for message in messages:
            try:
                print("Message received: {0}".format(message.body))
            except Exception as e:
                logging.error(e)
                print e
    

    
def _deleteMessage(message):
    message.delete()
    
    
      
def purge():
    try:
        client.purge_queue(QueueUrl=QUEUE_URL)
    except Exception as e:
        logging.error(e)
        print e
    

def isThereMassage():
    messages = queue.receive_messages(WaitTimeSeconds=1)
    
    if(not messages) : 
        return False
    else:
        return True
    

def loadHTSCodesFromJson():
    with open("hts.txt") as f:
        HTS = []
        for line in f:
            HTS.append(line)
    num_lines = sum(1 for line in open('hts.txt'))
    print "[INFO] Loading %d HTS codes. Select option 1 to push them to AWS \n" % num_lines 
    f.close()
    return HTS
    




if __name__ == "__main__":

    HTS=loadHTSCodesFromJson()
    print "[INFO] 1.Write Message \n[INFO] 2.Purge The Queue \n[INFO] 3.Read All Messages \n[INFO] 4.Is List Empty"
    
    User_input=raw_input()
    if(int(User_input)==1):
        purge()
        try:
            for HTSCode in HTS:
                writeMessage(HTSCode)
        except Exception as e:
            logging.error(e)
            print e
          
    elif (int(User_input)==2):
        try:
            purge()
        except Exception as e:
            logging.error(e)
            print e
                
    elif (int(User_input)==3):
        try:
            readMessage()
        except Exception as e:
            logging.error(e)  
            
    elif (int(User_input)==4):
        try:
            print(isThereMassage())
        except Exception as e:
            logging.error(e)            
                              
    else:
        print"Wrong input.\nExisting the script..."
        sys.exit(0)
