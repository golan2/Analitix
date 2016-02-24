package com.hp.analitix.data.in;

import org.apache.commons.cli.ParseException;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by golaniz on 17/02/2016.
 */
public class KafkaConsumerHelper {

    private final CmdOpts cmdOpts;

    public KafkaConsumerHelper(CmdOpts cmdOpts) throws ParseException {
        this.cmdOpts = cmdOpts;
    }

    protected JavaStreamingContext createJavaStreamingContext(String appName) {
        SparkConf sparkConf = new SparkConf().setAppName(appName).setMaster(cmdOpts.getSparkMaster());
        return new JavaStreamingContext(sparkConf, new Duration(2000));
    }

    protected JavaPairReceiverInputDStream<String, String> createReceiverStream(JavaStreamingContext jssc) {
        Map<String, Integer> topicMap = new HashMap<>();
        for (String topicName : cmdOpts.getTopicNames()) {
            topicMap.put(topicName, cmdOpts.getNumberOfThreads());
        }
        return KafkaUtils.createStream(jssc, cmdOpts.getZkQuorum(), cmdOpts.getConsumerGroup(), topicMap);
    }

    protected void startAndWait(JavaStreamingContext jssc) throws IOException {
        jssc.start();
        int read = 0;
        while (read!='Q') {
            read = System.in.read();
        }
    }

    protected void stopAndClose(JavaStreamingContext jssc) {
        jssc.stop();
        jssc.close();
    }
}
