# SparkStreaming_DirectKafka_example
this is the application example for manament offsets in zookeeper when using kafka direct stream api in spark streaming.  

## Simply extract offset ranges from dstream

    kafkaMessages.transformToPair(
                new Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>() {
                    public JavaPairRDD<String, String> call(JavaPairRDD<String, String> rdd) throws Exception {
                        OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                        offsetRanges.set(offsets);
                        return rdd;
                    }
                }
        ).foreachRDD(
            new VoidFunction<JavaPairRDD<String, String>>() {
                    public void call(JavaPairRDD<String, String> rdd) throws Exception {
                        for (OffsetRange o : offsetRanges.get()) {
                            System.out.println(
                                    "topic: " + o.topic() + " , partition: " + o.partition() + " , fromOffset " + o.fromOffset() + " , untilOffset: " + o.untilOffset() + "\n\n"
                            );
                        }
                    }
                }
        );  
        
## Save the offsets into zookeeper

    final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<OffsetRange[]>();
    final CuratorFramework  curatorFramework = CuratorFrameworkFactory.newClient(zkHost, 10000, 1000, new RetryUntilElapsed(1000,1000));
    curatorFramework.start();
    final ObjectMapper objectMapper = new ObjectMapper();
    
    kafkaMessages.transformToPair(
                new Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>() {
                    public JavaPairRDD<String, String> call(JavaPairRDD<String, String> rdd) throws Exception {
                        OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                        offsetRanges.set(offsets);
                        return rdd;
                    }
                }
        ).foreachRDD(
            new VoidFunction<JavaPairRDD<String, String>>() {
                    public void call(JavaPairRDD<String, String> rdd) throws Exception {
                        for (OffsetRange o : offsetRanges.get()) {
                            System.out.println(
                                    "topic: " + o.topic() + " , partition: " + o.partition() + " , fromOffset " + o.fromOffset() + " , untilOffset: " + o.untilOffset() + "\n\n"
                            );
                            String nodePath = "/consumers/" + consumerGroup + "/offsets/" + o.topic()+ "/" + o.partition();
                            final byte[] offsetBytes = objectMapper.writeValueAsBytes(o.untilOffset());
                            if(curatorFramework.checkExists().forPath(nodePath)!=null){
                                curatorFramework.setData().forPath(nodePath,offsetBytes);
                            }else{
                                curatorFramework.create().creatingParentsIfNeeded().forPath(nodePath, offsetBytes);
                                curatorFramework.create().creatingParentsIfNeeded().forPath("/consumers/" + consumerGroup + "/owners/" + o.topic());
                            }
                        }
                    }
                }
        );  
        
        
## Reference Link
[shashidhare]http://shashidhare.com/spark/2016/03/05/zookeeper-offset-management-in-kafka-direct-stream.html
        
        

   
   
 
