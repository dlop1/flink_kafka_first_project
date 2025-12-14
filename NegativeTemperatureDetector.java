package spendreport;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Properties;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import com.fasterxml.jackson.databind.ObjectMapper;
import spendreport.InputMessage;
import spendreport.OutputMessage;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import static spendreport.Consumer.*;
import static spendreport.Producer.*;

public class NegativeTemperatureDetector {
	public static void main(String[] args) throws Exception {
        String inputTopic = "Temperatura";
        String outputTopic = "Alarm";
        String consumerGroup = "group";
        String address = "localhost:9092";
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        FlinkKafkaConsumer<InputMessage> flinkKafkaConsumer = createConsumerForTopic(
            inputTopic, address, consumerGroup);
        DataStream<InputMessage> inputStream = env.addSource(flinkKafkaConsumer);
        FlinkKafkaProducer<OutputMessage> flinkKafkaProducer = createProducer(
            outputTopic, address);

        inputStream.filter(new FilterFunction<InputMessage>() {
            @Override
            public boolean filter(InputMessage msg) throws Exception {
                return msg.temperature < 0;
	        }
	}).map(new MapFunction<InputMessage, OutputMessage>() {
        @Override
        public OutputMessage map(InputMessage iMsg) throws Exception {
            System.out.println("Alarm: " + iMsg.temperature);
            return new OutputMessage(iMsg.measurement_time, iMsg.temperature);
        }
	}).addSink(flinkKafkaProducer);

	env.execute();
	}
}
