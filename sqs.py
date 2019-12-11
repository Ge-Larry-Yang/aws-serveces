import boto3

sqs = boto3.resource('sqs')

# create queue

queue = sqs.create_queue(QueueName='Jamie',
                         Attributes={'DelaySeconds': '5'})

print(queue.url)
print(queue.attributes.get('DelaySeconds'))

# for queue in sqs.queues.all():
#     print(queue.url)


queue = sqs.get_queue_by_name(QueueName='test')
print(queue.url)

response = queue.send_message(MessageBody='please take a look')

for message in queue.receive_messages(MessageAttributeNames=['Author']):
    author_text = ''
    if message.message_attributes is not None:
        author_name = message.message_attributes.get('Author').get('StringValue')
        if author_name:
            author_text = '({0})'.format(author_name)

        print('Hello, {0}!{1}'.format(message.body, author_text))

        message.delete()



