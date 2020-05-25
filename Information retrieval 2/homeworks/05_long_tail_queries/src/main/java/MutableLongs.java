import java.util.Arrays;

public class MutableLongs {
    private final double[] posBasedParams = {0.41, 0.16, 0.11, 0.08, 0.06,
            0.05, 0.04, 0.03, 0.025, 0.025};

    public long shows = 0;
    public long clicks = 0;

    public long[] posShows = new long[5];
    public long[] posClicks = new long[5];

    public double posBasedShows = 0;

    public long cascadeShows = 0;
    public long cascadeClicks = 0;

    public long dbnShows = 0;
    public long dbnClicks = 0;
    public long dbnSatisfied = 0;

    public long posSum = 0;

    public MutableLongs() {
        Arrays.fill(posShows, 0);
        Arrays.fill(posClicks, 0);
    }

    public void incShows() {
        shows++;
    }

    public void incClicks(long i) {
        clicks += i;
    }

    public void incPos(int pos, byte click) {
        if (pos < 5) {
            posShows[pos]++;
            posClicks[pos] += click;
        }
    }

    public void incPosBasedShows(int pos) {
        double param;
        if (pos < 10) {
            param = posBasedParams[pos];
        } else {
            param = posBasedParams[posBasedParams.length - 1];
        }
        posBasedShows += param;
    }

    public void incCascadeShows(int pos, int minClickPos) {
        if (pos <= minClickPos) {
            cascadeShows++;
        }
    }

    public void incCascadeClicks(int pos, int minClickPos) {
        if (pos == minClickPos) {
            cascadeClicks++;
        }
    }

    public void incDBN(int pos, int lastClickPos, int click) {
        if (pos <= lastClickPos) {
            dbnShows++;
            dbnClicks += click;
            if (pos == lastClickPos) {
                dbnSatisfied++;
            }
        }
    }

    public void incPosSum (int pos) {
        posSum += pos;
    }

    public void updateAll(byte click, int pos, int minClickPos, int lastClickPos) {
        this.incShows();
        this.incClicks(click);
        this.incPos(pos, click);
        this.incPosBasedShows(pos);
        this.incCascadeShows(pos, minClickPos);
        this.incCascadeClicks(pos, minClickPos);
        this.incDBN(pos, lastClickPos, click);
        this.incPosSum(pos);
    }

    public StringBuilder makeValue(int queryId) {
        StringBuilder res = new StringBuilder();
        if (queryId >= 0) {
            res.append(queryId); res.append("\t");
        }
        res.append(shows); res.append("\t");
        res.append(clicks); res.append("\t");
        for (long posShow : posShows) {
            res.append(posShow);
            res.append("\t");
        }
        for (long posClick : posClicks) {
            res.append(posClick);
            res.append("\t");
        }
        res.append(posBasedShows); res.append("\t");
        res.append(cascadeShows); res.append("\t");
        res.append(cascadeClicks); res.append("\t");
        res.append(dbnShows); res.append("\t");
        res.append(dbnClicks); res.append("\t");
        res.append(dbnSatisfied); res.append("\t");
        res.append((double) posSum / shows);
        return res;
    }
}
