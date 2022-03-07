import boto3
import json
import sqs_message_data
QUEUE_NAME = 'test_queue'
ACCOUNT_ID = 'YOUR AWS ACCOUNT ID'
class SqsMessages:
    def __init__(self):
        self.sqs_client = boto3.client("sqs", region_name="us-west-2")

    def create_queue(self):
        try:
            response = self.sqs_client.create_queue(
                QueueName=QUEUE_NAME,
                Attributes={
                    "DelaySeconds": "0",
                    "VisibilityTimeout": "60",  # 60 seconds
                }
            )
            print("my-new-queue -> ", response)
        except:
            print('Error during create queue')

    def send_message(self):
        for message in sqs_message_data.message_data:
            response = self.sqs_client.send_message(
                QueueUrl=f"https://us-west-2.queue.amazonaws.com/{ACCOUNT_ID}/{QUEUE_NAME}",
                MessageBody=json.dumps(message))
            print("Message Sent: ", message)


    def receive_messages(self):
        sqs_client = boto3.client("sqs", region_name="us-west-2")
        response = self.sqs_client.receive_message(QueueUrl=f"https://sqs.us-west-2.amazonaws.com/{ACCOUNT_ID}/{QUEUE_NAME}",
                                       MaxNumberOfMessages=10, WaitTimeSeconds=10)
        print(f"Number of messages received: {len(response.get('Messages', []))}")

        for message in response.get("Messages", []):
            print("Message Retrieved: ", message)
            self.delete_message(message['ReceiptHandle'])

    def delete_message(self, receipt_handle):
        response = self.sqs_client.delete_message(
            QueueUrl=f"https://us-west-2.queue.amazonaws.com/{ACCOUNT_ID}/{QUEUE_NAME}",
            ReceiptHandle=receipt_handle,
        )
        print("Message Deleted: ", response)


create = SqsMessages()
create.create_queue()
create.send_message()
create.receive_messages()
