import time


from adp_common.pb import tracking_pb2
from adp_common.pb import xnad_pb2

from python_common.mq.kafka import KafkaClient

tracking_log =  tracking_pb2.TrackingLog()
tracking_log.event_time = int(time.time() * 1000)
tracking_log.event_code = xnad_pb2.EVENT_IMP

tracking_log.bid_info.req.req_id = 123142366
tracking_log.bid_info.req.os= 1
tracking_log.bid_info.req.osv = '1.2.0'
tracking_log.bid_info.req.brand = 'huawei'
tracking_log.bid_info.req.model = 'huawei meta'
tracking_log.bid_info.req.conn = 2
tracking_log.bid_info.req.ip = '192.168.1.23'


tracking_log.bid_info.resp.xn_bi.advid = 100
tracking_log.bid_info.resp.xn_bi.campid = 1000
tracking_log.bid_info.resp.xn_bi.planid = 10000
kafka_client = KafkaClient(['172.16.11.1:9092','172.16.11.252:9092','172.16.11.89:9092'],topic='adp_test')
tracking_log_event = tracking_log.SerializeToString()
print(tracking_log_event)
kafka_client.write(tracking_log_event)