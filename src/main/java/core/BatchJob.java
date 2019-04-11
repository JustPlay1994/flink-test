package core;

import core.oracle.OracleReader;
import core.oracle.OracleWriter;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Created by JustPlay1994 on 2019/4/3.
 * https://github.com/JustPlay1994
 */

public class BatchJob {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<Tuple2<Double, Double>> dataStream = env.addSource(new OracleReader());

        dataStream.map(i -> {
            Double jd84 = i.getField(0);
            Double wd84 = i.getField(1);
            return jd84 * wd84;
        }).print();


        dataStream.keyBy(0).print();

        dataStream.keyBy(0).max(0);

        dataStream.flatMap((FlatMapFunction<Tuple2<Double, Double>, Object>) (value, out) -> out.collect(value));

//        dataStream.flatMap((Integer i, Collector<Integer> out) ->{
//            out.collect(i + 1);
//        } ).returns(Types.INT);

        dataStream.addSink(new OracleWriter());
        env.execute("flink mysql demo");
    }

}
