package core.wordcount;

import core.mysql.JdbcReader;
import core.mysql.JdbcWriter;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by JustPlay1994 on 2019/4/3.
 * https://github.com/JustPlay1994
 */

public class BatchJob {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource dataStream = env.addSource(new JdbcReader());

        dataStream.addSink(new JdbcWriter());
        env.execute("flink mysql demo");

    }

}
