import time


from python_common.mq.kafka import KafkaClient



kafka_client = KafkaClient(['172.16.11.1:9092','172.16.11.252:9092','172.16.11.89:9092'],topic='adp_test',group_id='test_group')
d = kafka_client.read()
for i in kafka_client.read():
    print(i)
