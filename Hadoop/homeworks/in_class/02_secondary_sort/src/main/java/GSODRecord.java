public class GSODRecord {
    public int station;
    public int year;
    public int month;
    public int day;
    public float max_temp;
    public float min_temp;

    private static final float NO_TEMPERATURE = 5537.72f;

    public void parseFrom(String line) {
        station = Integer.valueOf(line.substring(0, 6));
        year = Integer.valueOf(line.substring(14, 18));
        month = Integer.valueOf(line.substring(18, 20));
        day = Integer.valueOf(line.substring(20, 22));
        max_temp = parseTemp(line, 102, 108);
        min_temp = parseTemp(line, 110, 116);
    }

    public boolean hasMaxTemp() {
        return Math.abs(max_temp - NO_TEMPERATURE) > 1;
    }

    public boolean hasMinTemp() {
        return Math.abs(min_temp - NO_TEMPERATURE) > 1;
    }

    private float parseTemp(String s, int from, int to) {
        String sub = s.substring(from, to).trim();
        float t = Float.valueOf(sub);
        return toCelsius(t);
    }

    private float toCelsius(float F) {
        return (F - 32) * 5 / 9;
    }
}
