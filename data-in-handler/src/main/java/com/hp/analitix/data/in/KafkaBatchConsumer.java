
package com.hp.analitix.data.in;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.cluster.Partition;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import kafka.serializer.StringDecoder;
import org.apache.commons.cli.ParseException;
import org.apache.commons.net.nntp.Threadable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by golaniz on 22/02/2016.
 */
public class KafkaBatchConsumer implements Serializable {

    public  static final String TOPIC          = "iris";
    public  static final int    PARTITION      = 0;
    public  static final int    CHUNK_SIZE     = 20000;
    private static final int    MAX_ITERATIONS = 100;
    public  static final Path   PATH           = new Path("/datasets/iris.data");

    public static final Class<String>        C_S  = String.class;
    public static final Class<StringDecoder> C_SD = StringDecoder.class;

    private final CmdOpts cmdOpts;
    private final String clientId;

    public KafkaBatchConsumer(CmdOpts cmdOpts) {
        this.cmdOpts = cmdOpts;
        this.clientId = cmdOpts.getConsumerGroup() + this.hashCode();
    }

    public static void main(String[] args) throws ParseException, InterruptedException {
        KafkaBatchConsumer runner = new KafkaBatchConsumer(new CmdOpts(args));
        runner.sparkBatchProcessing();
    }


    public void sparkBatchProcessing() throws InterruptedException {
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", cmdOpts.getKafkaMetadataBrokerList());
        Map<String, String> map = new HashMap<>();
        OffsetRange[] offsetRanges = {
                OffsetRange.create(TOPIC, PARTITION, 0, 10000),
        };

        JavaSparkContext context = getJavaSparkContext();
        JavaPairRDD<String, String> rdd = (JavaPairRDD<String, String>) KafkaUtils.createRDD(context, C_S, C_S, C_SD, C_SD, kafkaParams, offsetRanges );

        rdd.foreach(new VoidFunction<Tuple2<String, String>>() {
            @Override
            public void call(Tuple2<String, String> t) throws Exception {
                System.out.println("~~" + t);
            }
        });

        System.out.println("!!!"+rdd.count());

        Thread.sleep(5000);
        context.close();
    }

    private void write2hdfsNoSpark() {
        try {
            SimpleConsumer consumer = new SimpleConsumer(cmdOpts.getKafkaBrokersList(), 9092, 100000, 64 * 1024, clientId);
            long offset = cmdOpts.getKafkaOffset();
            boolean readMore = true;
            int index = 0;
            while (readMore) {
                FetchResponse fetchResponse = fetchNextChunk(consumer, offset);
                offset = processChunk(fetchResponse);
                long highWatermark = fetchResponse.highWatermark(TOPIC, PARTITION);
                readMore = offset < highWatermark && ++index < MAX_ITERATIONS && !fetchResponse.hasError();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private FetchResponse fetchNextChunk(SimpleConsumer consumer, long offset) {
        FetchRequestBuilder fetchRequestBuilder = new FetchRequestBuilder().clientId(clientId).addFetch(TOPIC, PARTITION, offset, CHUNK_SIZE);
        FetchRequest fetchRequest =  fetchRequestBuilder.build();
        return consumer.fetch(fetchRequest);
    }

    private long processChunk(FetchResponse fetchResponse) throws IOException {
        System.out.println("processChunk");
        FileSystem fileSystem = null;
        FSDataOutputStream outputStream = null;
        try {
            long lastOffset = -1;
            for (MessageAndOffset mao :  fetchResponse.messageSet(TOPIC, PARTITION)) {
                if (fileSystem==null) fileSystem = openFileSystemConnection();
                if (outputStream==null) outputStream = fileSystem.create(PATH);
                ByteBuffer payload = mao.message().payload();

                CharBuffer message = StandardCharsets.UTF_8.decode(payload);
                System.out.println("~~~Message ["+mao.offset()+"]: [" + message + "]");
                lastOffset = mao.nextOffset();
                outputStream.write((message.toString()+"\n").getBytes());
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

}
