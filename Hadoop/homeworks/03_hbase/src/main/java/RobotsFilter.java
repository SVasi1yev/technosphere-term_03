import java.util.ArrayList;
import java.util.regex.Pattern;

public class RobotsFilter {
    public class BadFormatException extends Exception {
        public BadFormatException(String explanation) {
            super(explanation);
        }
    }

    static final String START = "Disallow: ";

    private ArrayList<Pattern> patterns = new ArrayList<>();

    public RobotsFilter () {}
    public RobotsFilter (String robots) throws BadFormatException {
        if (robots.length() == 0) {
            return;
        }

        for (String line: robots.split("\n")) {
            if (!line.startsWith(START)) {
                throw new BadFormatException(line);
            }

            String patternsStr = line.substring(START.length());
            if (patternsStr.startsWith("/")) {
                patternsStr = "^\\Q" + patternsStr + "\\E.*";
            } else if (patternsStr.startsWith("*")) {
                patternsStr = patternsStr.substring(1);
                patternsStr = ".*\\Q" + patternsStr + "\\E.*";
            } else if (patternsStr.endsWith("$")) {
                patternsStr = ".*\\Q" + patternsStr + "\\E$";
            } else {
                throw new BadFormatException("Unknown rule!");
            }

            if (patternsStr.endsWith("$\\E.*")) {
                patternsStr = patternsStr.substring(0, patternsStr.length() - 5) + "\\E$";
            }

            patterns.add(Pattern.compile(patternsStr));
        }
    }

    public boolean IsDisable(String url) {
        return !IsAllowed(url);
    }

    public boolean IsAllowed(String url) {
        if (!url.startsWith("/")) {
            url = "/" + url;
        }

        for (Pattern p: patterns) {
            if (p.matcher(url).matches()) {
                return false;
            }
        }
        return true;
    }
}
