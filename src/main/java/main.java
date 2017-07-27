import org.apache.log4j.Logger;

/**
 * Created by exem on 2017-02-07.
 */
public class main {
    static final Logger logger = Logger.getLogger(main.class);

    public static void main( String [] args ) throws InterruptedException {

        logger.info("Hello! PercolatorApplication is started!!");

        String brokers = "10.10.0.62:9092";
        String inputTopics = "test_offset";
        String esAddrs = "http://10.10.0.222:9200";
        String zkHost = "10.10.0.215:2181";
        int kafkaMaxFetchBytes = 20971520; // 20MB

        process percolatorProcess = new process(brokers, kafkaMaxFetchBytes, inputTopics, zkHost);
        percolatorProcess.process();
    }
}
