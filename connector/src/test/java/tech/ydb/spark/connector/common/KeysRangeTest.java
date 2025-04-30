package tech.ydb.spark.connector.common;

import java.io.Serializable;

import org.junit.Assert;
import org.junit.Test;



/**
 *
 * @author Aleksandr Gorshenin
 */
public class KeysRangeTest {

    @Test
    public void equalsTest() {
        Serializable[] v1 = new Serializable[] {"A", 10, -1L};
        Serializable[] v2 = new Serializable[] {"A", 10, 1L};
        Serializable[] v3 = new Serializable[] {"A", 10, 1L};

        KeysRange r1 = new KeysRange(v1, true, v2, true);
        KeysRange r2 = new KeysRange(v1, true, v3, true);
        KeysRange r3 = new KeysRange(v1, true, v2, false);
        KeysRange r4 = new KeysRange(v2, true, v1, false);

        Assert.assertEquals(r1.hashCode(), r2.hashCode());
        Assert.assertNotEquals(r1.hashCode(), r3.hashCode());
        Assert.assertNotEquals(r1.hashCode(), KeysRange.EMPTY.hashCode());
        Assert.assertNotEquals(r1.hashCode(), KeysRange.UNRESTRICTED.hashCode());
        Assert.assertEquals(r4.hashCode(), KeysRange.EMPTY.hashCode());
        Assert.assertNotEquals(r4.hashCode(), KeysRange.UNRESTRICTED.hashCode());

        Assert.assertEquals(r1, r2);
        Assert.assertNotEquals(r1, r3);
        Assert.assertNotEquals(r1, KeysRange.EMPTY);
        Assert.assertNotEquals(r1, KeysRange.UNRESTRICTED);
        Assert.assertEquals(r4, KeysRange.EMPTY);
        Assert.assertNotEquals(r4, KeysRange.UNRESTRICTED);

        Assert.assertNotEquals(r1, null);
        Assert.assertNotEquals(r1, "String");
    }

    @Test
    public void compareValuesTest() {
        Serializable[] v1 = new Serializable[] {"A", 10, 1L};
        Serializable[] v2 = new Serializable[] {"A", 10, 1L};
        Serializable[] v3 = new Serializable[] {"A", 10, -1L};
        Serializable[] v4 = new Serializable[] {"A", 10};
        Serializable[] v5 = new Serializable[] {"A", 10, null};
        Serializable[] v6 = new Serializable[] {"A", 10, null, 10};

        Assert.assertEquals(0, KeysRange.compareValues(null, null));
        Assert.assertEquals(1, KeysRange.compareValues(v1, null));
        Assert.assertEquals(-1, KeysRange.compareValues(null, v1));

        Assert.assertEquals(0, KeysRange.compareValues(v1, v1));
        Assert.assertEquals(0, KeysRange.compareValues(v1, v2));

        Assert.assertEquals(1, KeysRange.compareValues(v1, v3));
        Assert.assertEquals(-1, KeysRange.compareValues(v3, v1));

        Assert.assertEquals(-1, KeysRange.compareValues(v4, v3));
        Assert.assertEquals(1, KeysRange.compareValues(v3, v4));
        Assert.assertEquals(-1, KeysRange.compareValues(v5, v3));
        Assert.assertEquals(1, KeysRange.compareValues(v3, v5));

        Assert.assertEquals(0, KeysRange.compareValues(v4, v5));
        Assert.assertEquals(0, KeysRange.compareValues(v5, v6));
    }

    @Test
    public void compareIncompatibleValuesTest() {
        Serializable[] v1 = new Serializable[] {"A"};
        Serializable[] v2 = new Serializable[] {10};

        Assert.assertEquals("Incompatible data types class java.lang.String and class java.lang.Integer",
                Assert.assertThrows(
                        IllegalArgumentException.class,
                        () -> KeysRange.compareValues(v1, v2)
                ).getMessage());
    }

    @Test
    public void compareUncoparableValuesTest() {
        Serializable[] v1 = new Serializable[] {new Object[] {10}};
        Serializable[] v2 = new Serializable[] {new Object[] {11}};

        Assert.assertEquals("Uncomparable data type class [Ljava.lang.Object;",
                Assert.assertThrows(
                        IllegalArgumentException.class,
                    () -> KeysRange.compareValues(v1, v2)
                ).getMessage());
    }

    @Test
    public void emptyTest() {
        Serializable[] v1 = new Serializable[] {"A", 10, -1L};
        Serializable[] v2 = new Serializable[] {"A", 10, 1L};

        Assert.assertFalse(KeysRange.UNRESTRICTED.isEmpty());
        Assert.assertFalse(new KeysRange(null, true, null, true).isEmpty());
        Assert.assertFalse(new KeysRange(v1, true, null, true).isEmpty());
        Assert.assertFalse(new KeysRange(null, true, v1, true).isEmpty());
        Assert.assertFalse(new KeysRange(v1, true, v2, true).isEmpty());
        Assert.assertFalse(new KeysRange(v1, true, v1, true).isEmpty());

        Assert.assertTrue(KeysRange.EMPTY.isEmpty());
        Assert.assertTrue(new KeysRange(v2, true, v1, true).isEmpty());
        Assert.assertTrue(new KeysRange(v1, false, v1, true).isEmpty());
        Assert.assertTrue(new KeysRange(v1, true, v1, false).isEmpty());
    }

    @Test
    public void toStringTest() {
        Serializable[] v1 = new Serializable[] {"A"};
        Serializable[] v2 = new Serializable[] {"B"};
        Serializable[] v3 = new Serializable[] {"A", 10, -1L};
        Serializable[] v4 = new Serializable[] {"A", 10, 1L};

        Assert.assertEquals("(-Inf - +Inf)", KeysRange.UNRESTRICTED.toString());
        Assert.assertEquals("(None)", KeysRange.EMPTY.toString());
        Assert.assertEquals("(-Inf - A]", new KeysRange(null, false, v1, true).toString());
        Assert.assertEquals("(-Inf - (A,10,-1))", new KeysRange(null, true, v3, false).toString());
        Assert.assertEquals("(B - +Inf)", new KeysRange(v2, false, null, true).toString());
        Assert.assertEquals("[(A,10,1) - +Inf)", new KeysRange(v4, true, null, false).toString());
    }

    @Test
    public void intersectTest() {
        Serializable[] v1 = new Serializable[] {"A", 9};
        Serializable[] v2 = new Serializable[] {"A", 10};
        Serializable[] v3 = new Serializable[] {"A", 10, 1L};
        Serializable[] v4 = new Serializable[] {"B"};

        KeysRange r1 = new KeysRange(v1, true, v2, true);     // [(A,9) - (A,10)]
        KeysRange r2 = new KeysRange(v1, false, v3, true);    // ((A,9) - (A,10,1)]
        KeysRange r3 = new KeysRange(v3, false, v4, false);   // ((A,10,1) - B)
        KeysRange r4 = new KeysRange(v3, true, null, false);  // [(A,10,1) - +Inf)
        KeysRange r5 = new KeysRange(null, false, v3, false); // (-Inf - (A,10,1))

        Assert.assertEquals(KeysRange.EMPTY, r1.intersect(r3));
        Assert.assertEquals(KeysRange.EMPTY, r3.intersect(r1));
        Assert.assertEquals(KeysRange.EMPTY, r2.intersect(r3));

        Assert.assertEquals("((A,9) - (A,10)]", r1.intersect(r2).toString());
        Assert.assertEquals("((A,9) - (A,10)]", r2.intersect(r1).toString());
        Assert.assertEquals("((A,10,1) - B)", r3.intersect(r4).toString());
        Assert.assertEquals("((A,10,1) - B)", r4.intersect(r3).toString());
        Assert.assertEquals("((A,9) - (A,10,1))", r2.intersect(r5).toString());
        Assert.assertEquals("((A,9) - (A,10,1))", r5.intersect(r2).toString());

        Assert.assertEquals("[(A,10,1) - (A,10,1)]", r2.intersect(r4).toString());
        Assert.assertEquals("[(A,10,1) - (A,10,1)]", r4.intersect(r2).toString());
        Assert.assertEquals(KeysRange.EMPTY, r3.intersect(r2));
        Assert.assertEquals(KeysRange.EMPTY, r4.intersect(r5));

        Assert.assertEquals(KeysRange.EMPTY, KeysRange.EMPTY.intersect(KeysRange.UNRESTRICTED));
        Assert.assertEquals(KeysRange.EMPTY, KeysRange.UNRESTRICTED.intersect(KeysRange.EMPTY));
        Assert.assertEquals(KeysRange.EMPTY, KeysRange.EMPTY.intersect(r1));
        Assert.assertEquals(KeysRange.EMPTY, r1.intersect(KeysRange.EMPTY));

        Assert.assertEquals(r1, r1.intersect(KeysRange.UNRESTRICTED));
        Assert.assertEquals(r2, KeysRange.UNRESTRICTED.intersect(r2));
    }
}
