package tech.ydb.spark.connector;

import org.junit.Assert;
import org.junit.Test;

import tech.ydb.spark.connector.impl.YdbWriterImpl;

/**
 *
 * @author zinal
 */
public class EscapeYqlIdsTest {

    @Test
    public void test1() {
        String input, result;

        input = "myfolder/mytable";
        result = YdbWriterImpl.escape(input);
        Assert.assertEquals(input, result);

        input = "myfolder`mytable";
        result = YdbWriterImpl.escape(input);
        Assert.assertEquals("myfolder\\`mytable", result);

        input = "table1`; DROP TABLE table2;";
        result = YdbWriterImpl.escape(input);
        Assert.assertEquals("table1\\`; DROP TABLE table2;", result);

        input = "strange\nidentifier\nwith\nnewlines\n";
        result = YdbWriterImpl.escape(input);
        Assert.assertEquals("strange\\nidentifier\\nwith\\nnewlines\\n", result);
    }

}
