import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.*;
import org.junit.Test;
import java.net.MalformedURLException;
import java.net.URL;

public class RobotsFilterTest {
    @Test
    public void testSimpleCase() throws RobotsFilter.BadFormatException {
        RobotsFilter filter = new RobotsFilter("Disallow: /users");

        assertTrue(filter.IsAllowed("/company/about.html"));

        assertFalse(filter.IsAllowed("/users/jan"));
        assertFalse(filter.IsAllowed("/users/"));
        assertFalse(filter.IsAllowed("/users"));

        assertTrue("should be allowed since in the middle", filter.IsAllowed("/another/prefix/users/about.html"));
        assertTrue("should be allowed since at the end", filter.IsAllowed("/another/prefix/users"));
    }

    @Test
    public void testURLLib() throws RobotsFilter.BadFormatException, MalformedURLException {

        String url_string = "http://www.roastandgrill.ru/#!product/prd1/2731965201/%D0%BB%D0%B5%D0%BF%D0%B5%D1%88%D0%BA%D0%B0";
        URL extractor = new URL(url_string);
        String file = extractor.getFile();
        String host = extractor.getHost();
        System.out.println(file);
        System.out.println(host);
        System.out.println(extractor.getAuthority());
        System.out.println(extractor.getPath());
        System.out.println(file + "#" + extractor.getRef());

        String url_string1 = "http://zmz-milk.ru/galereya/nagrady#!nagrady_zheleznogorsk_moloko__5";
        URL extractor1 = new URL(url_string1);
        String file1 = extractor1.getFile();
        String host1 = extractor1.getHost();
        System.out.println(file1);
        System.out.println(host1);
        System.out.println(extractor1.getAuthority());
        System.out.println(extractor1.getPath());
        System.out.println(file1 + "#" + extractor1.getRef());

        String url_string2 = "http://banktop.ru/bank/1139/1/";
        URL extractor2 = new URL(url_string2);
        String file2 = extractor2.getFile();
        String host2 = extractor2.getHost();
        System.out.println(file2);
        System.out.println(host2);
        System.out.println(extractor2.getAuthority());
        System.out.println(extractor2.getPath());
        System.out.println(file2 + "#" + extractor2.getRef());
    }


    @Test
    public void testEmptyCase() throws RobotsFilter.BadFormatException {
        RobotsFilter filter = new RobotsFilter();

        assertTrue(filter.IsAllowed("/company/about.html"));
        assertTrue(filter.IsAllowed("/company/second.html"));
        assertTrue(filter.IsAllowed("any_url"));
    }

    @Test
    public void testEmptyStringCase() throws RobotsFilter.BadFormatException {
        // that's different from testEmptyCase() since we
        // explicitly pass empty robots_txt rules
        RobotsFilter filter = new RobotsFilter("");

        assertTrue(filter.IsAllowed("/company/about.html"));
        assertTrue(filter.IsAllowed("/company/second.html"));
        assertTrue(filter.IsAllowed("any_url"));
    }

    @Test
    public void testRuleEscaping() throws RobotsFilter.BadFormatException {
        // we have to escape special characters in rules (like ".")
        RobotsFilter filter = new RobotsFilter("Disallow: *.php$");

        assertFalse(filter.IsAllowed("file.php"));
        assertTrue("sphp != .php", filter.IsAllowed("file.sphp"));
    }

    @Test(expected = RobotsFilter.BadFormatException.class)
    public void testBadFormatException() throws RobotsFilter.BadFormatException {
        RobotsFilter filter = new RobotsFilter("Allowed: /users");
    }

    @Test
    public void testAllCases() throws RobotsFilter.BadFormatException {
        String rules = "Disallow: /users\n" +
                "Disallow: *.php$\n" +
                "Disallow: */cgi-bin/\n" +
                "Disallow: /very/secret.page.html$\n";

        RobotsFilter filter = new RobotsFilter(rules);

        assertFalse(filter.IsAllowed("/users/jan"));
        assertTrue("should be allowed since in the middle", filter.IsAllowed("/subdir2/users/about.html"));

        assertFalse(filter.IsAllowed("/info.php"));
        assertTrue("we disallowed only the endler", filter.IsAllowed("/info.php?user=123"));
        assertTrue(filter.IsAllowed("/info.pl"));

        assertFalse(filter.IsAllowed("/forum/cgi-bin/send?user=123"));
        assertFalse(filter.IsAllowed("/forum/cgi-bin/"));
        assertFalse(filter.IsAllowed("/cgi-bin/"));
        assertTrue(filter.IsAllowed("/scgi-bin/"));

        assertFalse(filter.IsAllowed("/very/secret.page.html"));
        assertTrue("we disallowed only the whole match", filter.IsAllowed("/the/very/secret.page.html"));
        assertTrue("we disallowed only the whole match", filter.IsAllowed("/very/secret.page.html?blah"));
        assertTrue("we disallowed only the whole match", filter.IsAllowed("/the/very/secret.page.html?blah"));
    }

    static public void main(String[] args) throws Exception {
        RobotsFilterTest test = new RobotsFilterTest();
        try {
            test.testBadFormatException();
        } catch (RobotsFilter.BadFormatException exc) {
            System.out.println("BadFormat test passed!");

            test.testEmptyStringCase();
            test.testSimpleCase();
            test.testRuleEscaping();
            test.testAllCases();
            test.testEmptyCase();
            test.testURLLib();

            System.out.println("All passed!");
            return;
        }

        throw new Exception("Bad format failed!");
    }
}