


# 启动redis server
# rm -rf redis-6.0.8 
# wget http://download.redis.io/releases/redis-6.0.8.tar.gz
# tar -xzvf redis-6.0.8.tar.gz
# rm -rf redis-6.0.8.tar.gz
# cd redis-6.0.8
# make -j 10
# cd ..

nohup ./redis-6.0.8/src/redis-server > log/redis.log 2>&1 &
echo "启动redis数据库.."
sleep 5s

# 拒识服务
cd train
nohup python reject_infer.py > ../log/reject.log 2>&1 &
echo "启动拒识服务.."
sleep 5s

# 意图召回服务
nohup python intent_infer.py > ../log/intent.log 2>&1 &
echo "启动意图识别服务.."
sleep 5s

# 大模型nlu服务
cd ../function_call
nohup python chatnlu_infer.py > ../log/nlu.log 2>&1 &
echo "启动NLU服务.."
sleep 5s

# 入口服务 
cd ../
nohup python start.py > ./log/start.log 2>&1 &
echo "启动入口服务.."
sleep 5s

echo "done"

# wc -l any.txt
