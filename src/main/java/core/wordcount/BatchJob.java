package core.wordcount;

import core.mysql.JdbcReader;
import core.mysql.JdbcWriter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by JustPlay1994 on 2019/4/3.
 * https://github.com/JustPlay1994
 */

public class BatchJob {

    // *************************************************************************
    //     PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {


        // set up the execution environment
//        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(5000);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // source
        DataStreamSource dataStream = env.addSource(new JdbcReader());

        dataStream.addSink(new JdbcWriter());//写入mysql
        env.execute("flink mysql demo"); //运行程序

    }


}
