package core.oracle;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
 
/**
 * @Description mysql source
 * @Author jiangxiaozhi
 * @Date 2018/10/15 17:05
 **/
public class OracleReader extends RichSourceFunction<Tuple2<Double,Double>> {
    private static final Logger logger = LoggerFactory.getLogger(OracleReader.class);
 
    private Connection connection = null;
    private PreparedStatement ps = null;

    String username = "DAXING";
    String password = "123456";
    String url = "jdbc:oracle:thin:@10.217.17.71:1521/orcl";
    String driver = "oracle.jdbc.OracleDriver";
    String sql = "SELECT rowid row_id, jd84, wd84 from BLK_SANXIAO_PLACE";


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName(driver);
        connection = DriverManager.getConnection(url, username, password);
        ps = connection.prepareStatement(sql);
    }
 
    //执行查询并获取结果
    @Override
    public void run(SourceContext<Tuple2<Double, Double>> ctx) throws Exception {
        try {
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                double name = resultSet.getDouble("jd84");
                double id = resultSet.getDouble("wd84");
                logger.error("readJDBC name:{}", name);
                Tuple2<Double,Double> tuple2 = new Tuple2<>();
                tuple2.setFields(id,name);
                ctx.collect(tuple2);
            }
        } catch (Exception e) {
            logger.error("runException:{}", e);
        }
 
    }
     
    //关闭数据库连接
    @Override
    public void cancel() {
        try {
            super.close();
            if (connection != null) {
                connection.close();
            }
            if (ps != null) {
                ps.close();
            }
        } catch (Exception e) {
            logger.error("runException:{}", e);
        }
    }
}