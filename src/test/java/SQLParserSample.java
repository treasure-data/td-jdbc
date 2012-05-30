import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

import com.treasure_data.jdbc.command.ExtCCSQLParser;
import com.treasure_data.jdbc.compiler.parser.CCSQLParser;
import com.treasure_data.jdbc.compiler.stat.Select;
import com.treasure_data.jdbc.compiler.stat.Show;
import com.treasure_data.jdbc.compiler.stat.Statement;


public class SQLParserSample {

    public static void main(String[] args) throws Exception {
        String sql = "select count(*) from table2;";

        //String sql = "show foo";
        //String sql = "show schemas";
        //String sql = "show tables";
        //String sql = "show tables error";
        //String sql = "show tables from schemaName";
        //String sql = "show columns from tableName";
        //String sql = "show columns from tableName from schemaName";
        //String sql = "show columns from tableName from schemaName error";
        ExtCCSQLParser p = new ExtCCSQLParser(sql);
        Statement stat = p.Statement();
        if (stat instanceof Select) {
            processSelect((Select) stat);
        } else if (stat instanceof Show) {
            processShow((Show) stat);
        } else {
            throw new RuntimeException("fatal error: " + stat);
        }
    }

    private static void processSelect(Select stat) {
        System.out.println("## " + stat);
    }

    private static void processShow(Show stat) {
        System.out.println(stat);
        System.out.println("type: " + stat.getType());
        List<String> params = stat.getParameters();
        if (params != null) {
            for (String p : params) {
                System.out.println("param: " + p);
            }
        }
    }
}
