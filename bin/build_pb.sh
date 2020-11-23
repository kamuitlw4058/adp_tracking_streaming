base_path=$(cd `dirname $0`; cd ..; pwd)
cd $base_path
cd ${base_path}/src/main/proto/
protoc tracking.proto  --python_out=../python/pb
protoc xnad.proto  --python_out=../python/pb
echo end build..