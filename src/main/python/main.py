import time


from pb import tracking_pb2
from pb import xnad_pb2

from python_common.mq.kafka import KafkaClient

tracking_log =  tracking_pb2.TrackingLog()
tracking_log.event_time = int(time.time())
tracking_log.event_code = xnad_pb2.EVENT_IMP

tracking_log.bid_info.os= 1
tracking_log.bid_info.osv = '1.2.0'
tracking_log.bid_info.brand = 'huawei'
tracking_log.bid_info.model = 'huawei meta'
tracking_log.bid_info.conn = 2
tracking_log.bid_info.ip = '192.168.1.23'


tracking_log.bid_info.xn_bi.advid = 100
tracking_log.bid_info.xn_bi.campid = 1000
tracking_log.bid_info.xn_bi.planid = 10000


kafka_client = KafkaClient('172.16.11.1:9092,172.16.11.252:9092,172.16.11.89:9092',topic='adp_test')
tracking_log_event = tracking_log.SerializeToString()
print(tracking_log_event)
kafka_client.write(tracking_log_event)
kafka_client.write("test")