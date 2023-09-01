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

        x1 = makeInclusive("A", 10, 1L);
        x2 = makeInclusive("A", 20, 1L);
        Assert.assertEquals(-1, x1.compareTo(x2, true));
        Assert.assertEquals(-1, x1.compareTo(x2, false));
        Assert.assertEquals(1, x2.compareTo(x1, true));
        Assert.assertEquals(1, x2.compareTo(x1, false));

        x2 = makeInclusive("A", 10, 1L);
        Assert.assertEquals(0, x1.compareTo(x2, true));

        x2 = makeInclusive("A", 10);
        Assert.assertEquals(1, x1.compareTo(x2, true));
        Assert.assertEquals(-1, x1.compareTo(x2, false));
        Assert.assertEquals(-1, x2.compareTo(x1, true));
        Assert.assertEquals(1, x2.compareTo(x1, false));
    }

    @Test
    public void testEmpty() {
        YdbKeyRange.Limit x1, x2;

        x1 = makeInclusive("A", 10, 1L);
        x2 = makeInclusive("A", 10, 1L);
        Assert.assertEquals(false, new YdbKeyRange(x1, x2).isEmpty());

        x1 = makeInclusive("A", 10, 1L);
        x2 = makeExclusive("A", 10, 1L);
        Assert.assertEquals(true, new YdbKeyRange(x1, x2).isEmpty());

        x1 = makeExclusive("A", 10, 1L);
        x2 = makeExclusive("A", 10, 2L);
        Assert.assertEquals(false, new YdbKeyRange(x1, x2).isEmpty());

        Assert.assertEquals(true,
                new YdbKeyRange(new Object[] {31000000L}, new Object[] {31000000L}).isEmpty());
    }

    @Test
    public void testIntersect() {
        YdbKeyRange r1, r2, ro;
        r1 = new YdbKeyRange(new Object[] {31000000L}, new Object[] {32000000L});
        r2 = new YdbKeyRange(new Object[] {46000000L}, new Object[] {46250000L});
        ro = r2.intersect(r1);
        Assert.assertEquals(true, ro.isEmpty());
    }

}
