import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class Session {
    String[] queryGeo;
    String[] shown;
    int[] clickedPos;
    int minClickPos;

    public Session() {}

    public Session(String session) {
        fromString(session);
    }

    private void fromString(String session) {
        String[] tabSplit = session.trim().split("\t");
        queryGeo = tabSplit[0].trim().split("@");
        shown = tabSplit[1].replace("http://","").replace("https://","").replace("www.", "").split(",");
        String[] clicked = tabSplit[2].replace("http://","").replace("https://","").replace("www.", "").split(",");
        for (int i = 0; i < shown.length; i++) {
            if (shown[i].endsWith("/")) {
                shown[i] = shown[i].substring(0, shown[i].length() - 1);
            }
        }
        for (int i = 0; i < clicked.length; i++) {
            if (clicked[i].endsWith("/")) {
                clicked[i] = clicked[i].substring(0, clicked[i].length() - 1);
            }
        }
        clickedPos = new int[clicked.length];
        for (int i = 0; i < clicked.length; i++) {
            for (int j = 0; j < shown.length; j++) {
                if (clicked[i].equals(shown[j])) {
                    clickedPos[i] = j;
                    break;
                }
            }
        }
        minClickPos = 100;
        for (int pos : clickedPos) {
            if (pos < minClickPos) {
                minClickPos = pos;
            }
        }
    }
}