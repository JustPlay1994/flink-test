package core.chart;

import core.oracle.OracleReader;
import core.oracle.OracleWriter;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * Created by JustPlay1994 on 2019/4/3.
 * https://github.com/JustPlay1994
 */

public class Chart {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();




        DataStreamSource<Tuple2<Double, Double>> dataStream = env.addSource(new OracleReader());
        dataStream.addSink(new OracleWriter());

        StreamTableEnvironment tEnv = StreamTableEnvironment.getTableEnvironment(env);



        tEnv.registerDataStream("BLK_SANXIAO_PLACE", dataStream);

        Table sanxiao = tEnv.scan("BLK_SANXIAO_PLACE");


        env.execute("flink mysql demo"); 
    }

}
