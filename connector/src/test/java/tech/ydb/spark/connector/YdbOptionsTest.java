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

        existing.put(YdbOptions.URL, "url1");
        existing.put(YdbOptions.AUTH_MODE, "META");
        referenced.put(YdbOptions.URL, "url1");
        referenced.put(YdbOptions.AUTH_MODE, "META");
        Assert.assertEquals(true, YdbOptions.optionsMatches(existing, referenced));

        referenced.put(YdbOptions.URL, "url2");
        Assert.assertEquals(false, YdbOptions.optionsMatches(existing, referenced));

        referenced.put(YdbOptions.URL, "url1");
        Assert.assertEquals(true, YdbOptions.optionsMatches(existing, referenced));

        referenced.put(YdbOptions.AUTH_MODE, "ENV");
        Assert.assertEquals(false, YdbOptions.optionsMatches(existing, referenced));

        referenced.put(YdbOptions.AUTH_MODE, "META");
        Assert.assertEquals(true, YdbOptions.optionsMatches(existing, referenced));

        referenced.put(YdbOptions.AUTH_MODE, "STATIC");
        referenced.put(YdbOptions.AUTH_LOGIN, "ivanoff");
        referenced.put(YdbOptions.AUTH_PASSWORD, "password1");
        Assert.assertEquals(false, YdbOptions.optionsMatches(existing, referenced));

        existing.put(YdbOptions.AUTH_MODE, "STATIC");
        existing.put(YdbOptions.AUTH_LOGIN, "ivanoff");
        existing.put(YdbOptions.AUTH_PASSWORD, "password1");
        Assert.assertEquals(true, YdbOptions.optionsMatches(existing, referenced));

        existing.put(YdbOptions.AUTH_PASSWORD, "password2");
        Assert.assertEquals(true, YdbOptions.optionsMatches(existing, referenced));

        referenced.put(YdbOptions.AUTH_LOGIN, "petroff");
        Assert.assertEquals(false, YdbOptions.optionsMatches(existing, referenced));
    }

}
