package core.config;

/**
 * Created by JustPlay1994 on 2019/4/11.
 * https://github.com/JustPlay1994
 */

public interface OracleOutputConfig {

    String USERNAME = "TEST";
    String PASSWORD = "123456";
    String URL = "jdbc:oracle:thin:@10.217.17.71:1521/orcl";
    String DRIVER = "oracle.jdbc.OracleDriver";
    String SQL = "INSERT INTO BLK_SANXIAO_PLACE(object_id, jd84, wd84) values(?, ?, ?) ";

}
