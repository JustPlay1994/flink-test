package core.oracle;

import java.util.Date;

/**
 * Created by JustPlay1994 on 2019/4/8.
 * https://github.com/JustPlay1994
 */

public class IdGenerator {

    private static int id = (int) System.currentTimeMillis();

    synchronized  public  static int getId(){
        id++;
        return Math.abs(id);
    }

}
