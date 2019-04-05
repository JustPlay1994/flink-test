package core.mysql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
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
public class JdbcReader extends RichSourceFunction<Tuple2<String,String>> {
    private static final Logger logger = LoggerFactory.getLogger(JdbcReader.class);
 
    private Connection connection = null;
    private PreparedStatement ps = null;


    String username = "root";
    String password = "123456";
    String url = "jdbc:mysql://localhost:3306/test?autoReconnect=true&useSSL=false&user="+username+"&password="+password;
    String driver = "com.mysql.jdbc.Driver";
    String sql = "select *  from user1";


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName(driver);
        connection = DriverManager.getConnection(url);
        ps = connection.prepareStatement(sql);
    }
 
    //执行查询并获取结果
    @Override
    public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
        try {
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                String name = resultSet.getString("account");
                String id = resultSet.getString("password");
                logger.error("readJDBC name:{}", name);
                Tuple2<String,String> tuple2 = new Tuple2<>();
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