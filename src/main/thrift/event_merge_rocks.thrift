namespace java com.xiaoniuhy.adp.thrift

include 'nx_adp.thrift'

enum EventType {
   Request, //请求
   Impression,   //展现
   Click,  //点击
   Landing, //转化
}

struct TrackingEvent {
    1: required string request_id,
    2: required EventType event_type,
    3: required string user_id,
    4: required i64 timestamp,
}

struct MergedEvent{
    1: required string request_id,
    2: required string user_id,
    3: required list<i64> imp_list,
    4: required list<i64> clk_list,
    5: required list<i64> land_list,
    6: required i32 merge_count;
    7: required i64 last_update;
    8: required map<string,string> extendMap,
}



service  EventMergeService {
  MergedEvent singleEvent(1:TrackingEvent event)
  list<MergedEvent> batchEvent(1:list<TrackingEvent> events)
}

service LogEventService{
    oneway void batchEvent(1:string topic, 2: list<binary> logs);
   oneway void singleEvent(1:string topic, 2: binary logs);
}
