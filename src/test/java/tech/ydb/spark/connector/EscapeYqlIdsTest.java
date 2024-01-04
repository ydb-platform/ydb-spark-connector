package tech.ydb.spark.connector;

import org.junit.Assert;
import org.junit.Test;
import static tech.ydb.spark.connector.YdbWriterBasic.escape;

/**
 *
 * @author zinal
 */
public class EscapeYqlIdsTest {

    @Test
    public void test1() {
        String input, result;

        input = "myfolder/mytable";
        result = escape(input);
        Assert.assertEquals(input, result);

        input = "myfolder`mytable";
        result = escape(input);
        Assert.assertEquals("myfolder\\`mytable", result);

        input = "table1`; DROP TABLE table2;";
        result = escape(input);
        Assert.assertEquals("table1\\`; DROP TABLE table2;", result);

        input = "strange\nidentifier\nwith\nnewlines\n";
        result = escape(input);
        Assert.assertEquals("strange\\nidentifier\\nwith\\nnewlines\\n", result);
    }

}
