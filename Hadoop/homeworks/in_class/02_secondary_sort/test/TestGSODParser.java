import static org.junit.Assert.*;
import org.junit.Test;

public class TestGSODParser {
    @Test
    public void testSimpleLine() {
        GSODRecord rec = new GSODRecord();
        rec.parseFrom("010010 99999  20010102    19.7  7    16.9  7  1002.9  7  1001.7  7   13.6  6   11.3  7   14.0  999.9    21.6*   17.1   0.05F 999.9  001000");

        assertEquals(10010, rec.station);
        assertEquals(2001, rec.year);
        assertEquals(1, rec.month);
        assertEquals(2, rec.day);

        assertTrue(rec.hasMinTemp());
        assertTrue(rec.hasMaxTemp());

        assertEquals(-8.3, rec.min_temp, 0.1);
        assertEquals(-5.7, rec.max_temp, 0.1);
    }

    @Test
    public void testNoTemperatureLine() {
        GSODRecord rec = new GSODRecord();
        rec.parseFrom("010010 99999  20010102    19.7  7    16.9  7  1002.9  7  1001.7  7   13.6  6   11.3  7   14.0  999.9  9999.9* 9999.9   0.05F 999.9  001000");

        assertEquals(10010, rec.station);
        assertEquals(2001, rec.year);
        assertEquals(1, rec.month);
        assertEquals(2, rec.day);

        assertFalse(rec.hasMinTemp());
        assertFalse(rec.hasMaxTemp());
    }
}
