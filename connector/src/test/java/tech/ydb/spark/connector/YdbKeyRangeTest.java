package tech.ydb.spark.connector;

import java.io.Serializable;

import org.junit.Assert;
import org.junit.Test;

import tech.ydb.spark.connector.common.KeysRange;

/**
 *
 * @author mzinal
 */
public class YdbKeyRangeTest {

    private KeysRange.Limit makeExclusive(Serializable... vals) {
        return new KeysRange.Limit(vals, false);
    }

    private KeysRange.Limit makeInclusive(Serializable... vals) {
        return new KeysRange.Limit(vals, true);
    }

    @Test
    public void testCompare() {
        KeysRange.Limit x1;
        KeysRange.Limit x2;

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
        KeysRange.Limit x1;
        KeysRange.Limit x2;

        x1 = makeInclusive("A", 10, 1L);
        x2 = makeInclusive("A", 10, 1L);
        Assert.assertEquals(false, new KeysRange(x1, x2).isEmpty());

        x1 = makeInclusive("A", 10, 1L);
        x2 = makeExclusive("A", 10, 1L);
        Assert.assertEquals(true, new KeysRange(x1, x2).isEmpty());

        x1 = makeExclusive("A", 10, 1L);
        x2 = makeExclusive("A", 10, 2L);
        Assert.assertEquals(false, new KeysRange(x1, x2).isEmpty());

        Assert.assertEquals(true,
                new KeysRange(new Serializable[] {31000000L}, true, new Serializable[] {31000000L}, false).isEmpty());
    }

    @Test
    public void testIntersect() {
        KeysRange r1;
        KeysRange r2, ro;
        r1 = new KeysRange(new Serializable[] {31000000L}, true, new Serializable[] {32000000L}, false);
        r2 = new KeysRange(new Serializable[] {46000000L}, true, new Serializable[] {46250000L}, false);
        ro = r2.intersect(r1);
        Assert.assertEquals(true, ro.isEmpty());
    }

}
