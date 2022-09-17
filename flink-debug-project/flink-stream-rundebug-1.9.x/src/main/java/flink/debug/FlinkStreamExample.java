package flink.debug;

import flink.comm.FlinkExampleCommon;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.Test;

import java.util.Properties;

public class FlinkStreamExample extends FlinkExampleCommon {

    public static void main(String[] args) {

        ParameterTool param = ParameterTool.fromArgs(args);

//        String bootstrapServices = param.get("bootstrapServices", "localhost:9092");
//        String topic = param.get("topic", "testRecords");

        new FlinkStreamExample().doFlinkKafkaStreamExample(param);


    }

    @Test
    public void testMain() {
        new FlinkStreamExample().doFlinkKafkaStreamExample(null);
    }


    @Test
    public void testKafkaSource() throws Exception {
        StreamExecutionEnvironment streamEnv = getStreamEnv();
        Properties  kafkaProps = new Properties();
        kafkaProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        runFlinkKafkaConsumerDemo(streamEnv, "bdnode102:9092", "testSinkTopic", kafkaProps);
    }

    @Test
    public void testFlinkKafkaConsumerApi() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(0L);
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties  kafkaProps = new Properties();
        kafkaProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        kafkaProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

//        runFlinkKafkaConsumerDemo(env, "bdnode102:9092", "testSourceTopic", kafkaProps);
//        runFlinkKafkaConsumerDemo(env, "bdnode102:9092", "testSourceTopic", kafkaProps);
        runFlinkKafka2KafkaDemo(env, "bdnode102:9092", "testSourceTopic", "testSinkTopic", kafkaProps);

    }

    @Test
    public void testFlinkKafka2KafkaDemo() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(0L);
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties  kafkaProps = new Properties();
        kafkaProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        kafkaProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

//        runFlinkKafkaConsumerDemo(env, "bdnode102:9092", "testSourceTopic", kafkaProps);
        runFlinkKafka2KafkaDemo(env, "bdnode102:9092", "testSourceTopic", "testSinkTopic", kafkaProps);

    }


}
