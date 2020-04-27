import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Session {
    public String query;
    public int geo;
    public ArrayList<String> shown = new ArrayList<>();
    public ArrayList<String> clicked = new ArrayList<>();
    public ArrayList<Integer> shownPositions = new ArrayList<>();
    public ArrayList<Integer> clickedPositions = new ArrayList<>();

    public Session() {}

    public Session(String session, HashMap<String, Integer> urlsMap) {
        this.fromString(session, urlsMap);
    }

    public static void extractUrls(String in, HashMap<String, Integer> urlsMap, ArrayList<String> urls, ArrayList<Integer> positions) {
        int pos = 1;
        for (String s: in.split("http://|https://")) {
            if (s.length() == 0) {
                continue;
            }
            if (s.endsWith(",")) {
                s = s.substring(0, s.length() - 1);
            }
            if (urlsMap.containsKey(s)) {
                urls.add(s);
                positions.add(pos);
                continue;
            } else if (urlsMap.containsKey("www." + s)) {
                urls.add("www." + s);
                positions.add(pos);
                continue;
            }
            int lastCommaIdx = s.lastIndexOf(',');
            while (lastCommaIdx != -1) {
                s = s.substring(0, lastCommaIdx);
                if (urlsMap.containsKey(s)) {
                    urls.add(s);
                    positions.add(pos);
                    break;
                } else if (urlsMap.containsKey("www." + s)) {
                    urls.add("www." + s);
                    positions.add(pos);
                    break;
                }
                lastCommaIdx = s.lastIndexOf(',');
            }
            pos++;
        }
    }

    public void fromString(String session, HashMap<String, Integer> urlsMap) {
        String[] splitedByTab = session.split("\t");
        String[] splitedByAT = splitedByTab[0].split("@");
        query = splitedByAT[0];
        geo = Integer.parseInt(splitedByAT[1]);
        extractUrls(splitedByTab[1], urlsMap, shown, shownPositions);
        extractUrls(splitedByTab[2], urlsMap, clicked, clickedPositions);
        for (String s: shown) {
            System.out.print(s + " --- ");
        }
        System.out.print("\n");
    }
}
