public class Point {
    public double x, y;

    Point(double x, double y) {
        this.x = x;
        this.y = y;
    }

    synchronized public void increment() {
        x++; y++;
    }

    public String toString() {
        return "(" + x + "," + y + ") ";
    }
}
