package tech.ydb.spark.connector;

import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author zinal
 */
public class YdbOptionsTest {

    @Test
    public void testMatches() {
        Map<String, String> existing, referenced;

        existing = new HashMap<>();
        referenced = new HashMap<>();

        existing.put(YdbOptions.YDB_URL, "url1");
        existing.put(YdbOptions.YDB_AUTH_MODE, "META");
        referenced.put(YdbOptions.YDB_URL, "url1");
        referenced.put(YdbOptions.YDB_AUTH_MODE, "META");
        Assert.assertEquals(true, YdbOptions.connectionMatches(existing, referenced));

        referenced.put(YdbOptions.YDB_URL, "url2");
        Assert.assertEquals(false, YdbOptions.connectionMatches(existing, referenced));

        referenced.put(YdbOptions.YDB_URL, "url1");
        Assert.assertEquals(true, YdbOptions.connectionMatches(existing, referenced));

        referenced.put(YdbOptions.YDB_AUTH_MODE, "ENV");
        Assert.assertEquals(false, YdbOptions.connectionMatches(existing, referenced));

        referenced.put(YdbOptions.YDB_AUTH_MODE, "META");
        Assert.assertEquals(true, YdbOptions.connectionMatches(existing, referenced));

        referenced.put(YdbOptions.YDB_AUTH_MODE, "STATIC");
        referenced.put(YdbOptions.YDB_AUTH_LOGIN, "ivanoff");
        referenced.put(YdbOptions.YDB_AUTH_PASSWORD, "password1");
        Assert.assertEquals(false, YdbOptions.connectionMatches(existing, referenced));

        existing.put(YdbOptions.YDB_AUTH_MODE, "STATIC");
        existing.put(YdbOptions.YDB_AUTH_LOGIN, "ivanoff");
        existing.put(YdbOptions.YDB_AUTH_PASSWORD, "password1");
        Assert.assertEquals(true, YdbOptions.connectionMatches(existing, referenced));

        existing.put(YdbOptions.YDB_AUTH_PASSWORD, "password2");
        Assert.assertEquals(true, YdbOptions.connectionMatches(existing, referenced));

        referenced.put(YdbOptions.YDB_AUTH_LOGIN, "petroff");
        Assert.assertEquals(false, YdbOptions.connectionMatches(existing, referenced));
    }

}
