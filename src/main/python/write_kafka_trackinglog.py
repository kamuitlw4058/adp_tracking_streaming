import time

import random
from adp_common.pb import tracking_pb2
from adp_common.pb import xnad_pb2

from python_common.mq.kafka import KafkaClient


kafka_client = KafkaClient(['172.16.11.1:9092','172.16.11.252:9092','172.16.11.89:9092'],topic='adp_test')

def build_record(req_id,os,osv,brand,model,is_clk=False):

    tracking_log =  tracking_pb2.TrackingLog()
    tracking_log.event_time = int(time.time() * 1000)
    if is_clk:
        tracking_log.event_code = xnad_pb2.EVENT_CLICK
    else:
        tracking_log.event_code = xnad_pb2.EVENT_IMP

    tracking_log.bid_info.req.req_id = req_id
    tracking_log.bid_info.req.os= os
    tracking_log.bid_info.req.osv = osv
    tracking_log.bid_info.req.brand =  brand
    tracking_log.bid_info.req.model = model
    tracking_log.bid_info.req.conn = random.choice([1,2])
    tracking_log.bid_info.req.ip = random.choice( [ f'192.168.1.{i}' for i in range(10)] )

    tracking_log.bid_info.resp.xn_bi.advid = 100
    tracking_log.bid_info.resp.xn_bi.campid = 1000
    tracking_log.bid_info.resp.xn_bi.planid = 10000
    tracking_log_event = tracking_log.SerializeToString()
    kafka_client.write(tracking_log_event)


def build_record_with_clk(req_id,clk_rate = 0.1):
    has_clk = random.random()
    os = random.choice([1,0])
    osv = random.choice(['1.2.0','1.2.1'])
    brand = random.choice(['huawei','oppo','apple'])
    model = random.choice([ f'{i} model' for i in  ['huawei','oppo','apple']])

    build_record(req_id,os,osv,brand,model)

    if has_clk < clk_rate:
        build_record(req_id,os,osv,brand,model,is_clk=True)



for i in range(100100,101000):
    build_record_with_clk(i)