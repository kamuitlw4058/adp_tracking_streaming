base_path=$(cd `dirname $0`; cd ..; pwd)
cd $base_path
export SPARK_CLASSPATH=$base_path/lib:$SPARK_CLASSPATH
datetime=`date '+%Y%m%d%H%M%S'`
/opt/spark-2.4.5-hadoop3.1-1.0.2/bin/spark-submit target/tracking_streaming-1.0-SNAPSHOT.jar --jars lib/protobuf-java-3.8.0.jar


