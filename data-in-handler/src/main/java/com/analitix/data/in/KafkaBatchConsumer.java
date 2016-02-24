
package com.analitix.data.in;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import kafka.serializer.StringDecoder;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by golaniz on 22/02/2016.
 */
public class KafkaBatchConsumer implements Serializable {
    private static final String               CLA_CONSUMER_GROUP     = "consumerGroup";
    private static final String               CLA_KAFKA_BROKERS_LIST = "consumerGroup";
    private static final String               CLA_KAFKA_OFFSET       = "kafkaOffset";
    private static final String               CLA_KAFKA_TOPIC        = "kafkaTopic";
    private static final String               CLA_MAX_ITERATIONS     = "maxIterations";
    private static final String               CLA_HDFS_OUTPUT_PATH   = "hdfsOutputPath";
    private static final int                  PARTITION              =     0;
    private static final int                  CHUNK_SIZE             = 20000;
    public  static final Class<String>        C_S                    = String.class;
    public  static final Class<StringDecoder> C_SD                   = StringDecoder.class;

    private final CmdOpts cmdOpts;
    private final String clientId;

    public KafkaBatchConsumer(String[] args) throws ParseException {
        this.cmdOpts = new CmdOpts(args, getParamsMap(), Collections.emptySet());
        this.clientId = cmdOpts.get(CLA_CONSUMER_GROUP) + this.hashCode();
    }

    public static void main(String[] args) throws ParseException, InterruptedException {
        KafkaBatchConsumer runner = new KafkaBatchConsumer(args);
        runner.write2hdfsNoSpark();
    }

    private static Map<String, String> getParamsMap() {
        HashMap<String, String> result = new HashMap<>();
        result.put(CLA_CONSUMER_GROUP, "ConsumerGroup_Izik");
        result.put(CLA_KAFKA_BROKERS_LIST, "myd-vm23458.hpswlabs.adapps.hp.com");
        result.put(CLA_KAFKA_OFFSET, "0");
        result.put(CLA_KAFKA_TOPIC, "iris");
        result.put(CLA_MAX_ITERATIONS, "10000");
        result.put(CLA_HDFS_OUTPUT_PATH, "/datasets_izik/irisLibSVM.data");
        return result;
    }

    private void write2hdfsNoSpark() {
        try {
            SimpleConsumer consumer = new SimpleConsumer(cmdOpts.get(CLA_KAFKA_BROKERS_LIST), 9092, 100000, 64 * 1024, clientId);
            long offset = Long.parseLong(cmdOpts.get(CLA_KAFKA_OFFSET));
            boolean readMore = true;
            int chunks = 0;
            System.out.println("!!write2hdfsNoSpark - BEGIN");
            while (readMore) {
                FetchResponse fetchResponse = fetchNextChunk(consumer, offset);
                offset = processChunk(fetchResponse);
                long highWatermark = fetchResponse.highWatermark(cmdOpts.get(CLA_KAFKA_TOPIC), PARTITION);
                readMore = ++chunks < Integer.parseInt(cmdOpts.get(CLA_MAX_ITERATIONS)) && offset < highWatermark && !fetchResponse.hasError();
            }
            System.out.println("!!write2hdfsNoSpark - END chunks=["+chunks+"]");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private FetchResponse fetchNextChunk(SimpleConsumer consumer, long offset) {
        FetchRequestBuilder fetchRequestBuilder = new FetchRequestBuilder().clientId(clientId).addFetch(cmdOpts.get(CLA_KAFKA_TOPIC), PARTITION, offset, CHUNK_SIZE);
        FetchRequest fetchRequest =  fetchRequestBuilder.build();
        return consumer.fetch(fetchRequest);
    }

    private long processChunk(FetchResponse fetchResponse) throws IOException {
        FileSystem fileSystem = null;
        FSDataOutputStream outputStream = null;
        boolean consoleOnly = cmdOpts.get(CLA_HDFS_OUTPUT_PATH).equals("consoleOnly");
        try {
            long lastOffset = -1;
            for (MessageAndOffset mao :  fetchResponse.messageSet(cmdOpts.get(CLA_KAFKA_TOPIC), PARTITION)) {
                ByteBuffer payload = mao.message().payload();
                CharBuffer message = StandardCharsets.UTF_8.decode(payload);
                System.out.println("!!\tMessage ["+mao.offset()+"]: [" + message + "]");
                lastOffset = mao.nextOffset();
                if (!consoleOnly) {
                    if (fileSystem == null) fileSystem = openFileSystemConnection();
                    if (outputStream == null) outputStream = fileSystem.create(new Path(cmdOpts.get(CLA_HDFS_OUTPUT_PATH)));
                    outputStream.write((message.toString() + "\n").getBytes());
                }
            }
            return lastOffset;
        } finally {
            if (fileSystem != null) {
                fileSystem.close();
            }
            if (outputStream != null) {
                outputStream.close();
            }
        }
    }

    protected static FileSystem openFileSystemConnection() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://myd-vm22661.hpswlabs.adapps.hp.com:9000");
        conf.set("dfs.replication", "1");
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        return FileSystem.get(conf);
    }

    public static SparkContext getSparkContext() {
        return new SparkContext(new SparkConf().setAppName(KafkaBatchConsumer.class.getSimpleName()).setMaster("local[*]"));
    }

    protected static JavaSparkContext getJavaSparkContext() {
        return new JavaSparkContext(getSparkContext());
    }

//    public void sparkBatchProcessing() throws InterruptedException {
//        Map<String, String> kafkaParams = new HashMap<>();
//        kafkaParams.put("metadata.broker.list", cmdOpts.getKafkaMetadataBrokerList());
//        Map<String, String> map = new HashMap<>();
//        OffsetRange[] offsetRanges = {
//                OffsetRange.create(TOPIC, PARTITION, 0, 10000),
//        };
//
//        JavaSparkContext context = getJavaSparkContext();
//        JavaPairRDD<String, String> rdd = (JavaPairRDD<String, String>) KafkaUtils.createRDD(context, C_S, C_S, C_SD, C_SD, kafkaParams, offsetRanges );
//
//        rdd.foreach(new VoidFunction<Tuple2<String, String>>() {
//            @Override
//            public void call(Tuple2<String, String> t) throws Exception {
//                System.out.println("~~" + t);
//            }
//        });
//
//        System.out.println("!!!"+rdd.count());
//
//        Thread.sleep(5000);
//        context.close();
//    }
}
