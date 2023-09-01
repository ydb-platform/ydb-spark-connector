package tech.ydb.spark.connector;

import java.util.Arrays;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author mzinal
 */
public class YdbKeyRangeTest {

    private YdbKeyRange.Limit makeExclusive(Object... vals) {
        return new YdbKeyRange.Limit(Arrays.asList(vals), false);
    }

    private YdbKeyRange.Limit makeInclusive(Object... vals) {
        return new YdbKeyRange.Limit(Arrays.asList(vals), true);
    }

    @Test
    public void testCompare() {
        YdbKeyRange.Limit x1, x2;

        x1 = makeExclusive("A", 10, 1L);
        x2 = makeExclusive("A", 20, 1L);
        Assert.assertEquals(-1, x1.compareTo(x2, true));
        Assert.assertEquals(-1, x1.compareTo(x2, false));
        Assert.assertEquals(1, x2.compareTo(x1, true));
        Assert.assertEquals(1, x2.compareTo(x1, false));

        x2 = makeExclusive("A", 10, 1L);
        Assert.assertEquals(0, x1.compareTo(x2, true));

        x2 = makeExclusive("A", 10);
        Assert.assertEquals(1, x1.compareTo(x2, true));
        Assert.assertEquals(-1, x1.compareTo(x2, false));
        Assert.assertEquals(-1, x2.compareTo(x1, true));
        Assert.assertEquals(1, x2.compareTo(x1, false));
    }

}
