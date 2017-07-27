import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.serializer.StringDecoder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by exem on 2017-07-26.
 */
public class process  implements Serializable {
    static final Logger logger = Logger.getLogger(process.class);

    String kafkaBrokers;
    int kafkaMaxFetchBytes;
    String kafkaInputTopics;
    String zkHost;

    final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<OffsetRange[]>();
    CuratorFramework  curatorFramework;
    final ObjectMapper objectMapper = new ObjectMapper();

    public process(String kafkaBrokers, int kafkaMaxFetchBytes, String kafkaInputTopics, String zkHost) {
        this.kafkaBrokers = kafkaBrokers;
        this.kafkaMaxFetchBytes = kafkaMaxFetchBytes;
        this.kafkaInputTopics = kafkaInputTopics;
        this.zkHost = zkHost;
        curatorFramework = CuratorFrameworkFactory.newClient(zkHost, 10000, 1000, new RetryUntilElapsed(1000,1000));
        curatorFramework.start();
    }
    public process(String kafkaBrokers, String kafkaInputTopics, String zkHost) {
        this.kafkaBrokers = kafkaBrokers;
        this.kafkaInputTopics = kafkaInputTopics;
        this.zkHost = zkHost;
    }

    public void process() {

        SparkConf conf = new SparkConf().setAppName("PercolatorApplication");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        //jssc.checkpoint("/tmp");

        // Kafka setting
        Set<String> topicsSet = new HashSet<String>(Arrays.asList(kafkaInputTopics.split(",")));
        Map<String,String> kafkaParams = new HashMap<String,String>();
        kafkaParams.put("metadata.broker.list", kafkaBrokers);
        kafkaParams.put("fetch.message.max.bytes", String.valueOf(kafkaMaxFetchBytes));

        // DStream from Kafka
        JavaPairInputDStream<String, String> kafkaMessages = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicsSet
        );

        // DStream transformation

        // transform kafka message to percolating Metric object

        kafkaMessages.print();



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
                            String nodePath = "/consumers/testgroup/offsets/" + o.topic()+ "/" + o.partition();
                            final byte[] offsetBytes = objectMapper.writeValueAsBytes(o.untilOffset());
                            if(curatorFramework.checkExists().forPath(nodePath)!=null){
                                curatorFramework.setData().forPath(nodePath,offsetBytes);
                            }else{
                                curatorFramework.create().creatingParentsIfNeeded().forPath(nodePath, offsetBytes);
                            }
                        }
                    }
                }
        );


        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            logger.error(e.getStackTrace().toString());
        }
    }
}
