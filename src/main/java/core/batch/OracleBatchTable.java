package core.batch;

import core.config.OracleInputConfig;
import core.config.OracleOutputConfig;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Created by JustPlay1994 on 2019/4/11.
 * https://github.com/JustPlay1994
 */

public class OracleBatchTable {

    public static void main(String[] args) throws Exception {


//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        TableConfig tableConfig = new TableConfig();


        BatchTableEnvironment batchTableEnvironment = BatchTableEnvironment.getTableEnvironment(env);

        // Read data from a relational database using the JDBC input format
        DataSource<Row> dbData =
                env.createInput(
                        JDBCInputFormat.buildJDBCInputFormat()
                                .setDrivername(OracleInputConfig.DRIVER)
                                .setDBUrl(OracleInputConfig.URL)
                                .setQuery(OracleInputConfig.SQL)
                                .setUsername(OracleInputConfig.USERNAME)
                                .setPassword(OracleInputConfig.PASSWORD)
                                .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.BIG_DEC_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO))
                                .finish()
                );
//
//        dbData.print();
//        System.out.println(dbData.count());





//        Table table = batchTableEnvironment.fromDataSet(dbData, "object_id, jd84, wd84");
//
//
//
//        Table table = batchTableEnvironment.scan("sanxiao");

//        table.distinct().orderBy("object_id.asc").printSchema();

        dbData.sortPartition(0,Order.ASCENDING)
            .output(
                    // build and configure OutputFormat
                    JDBCOutputFormat.buildJDBCOutputFormat()
                            .setDrivername(OracleOutputConfig.DRIVER)
                            .setDBUrl(OracleOutputConfig.URL)
                            .setUsername(OracleOutputConfig.USERNAME)
                            .setPassword(OracleOutputConfig.PASSWORD)
                            .setQuery(OracleOutputConfig.SQL)
                            .finish()
            );

        env.execute("flink mysql demo");
    }

}
