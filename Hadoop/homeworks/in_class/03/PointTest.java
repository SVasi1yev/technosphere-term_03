public class PointTest extends Thread {
    Point point;

    public PointTest(Point point) {
        this.point = point;
    }

    public void run() {
        for (int i = 0; i < 10000; i++) {
            point.increment();
        }
    }

    public static void main(String args[]){
        Point myPoint = new Point(0.0, 0.0);
        PointTest myThread1 = new PointTest(myPoint);
        PointTest myThread2 = new PointTest(myPoint);
        myThread1.start(); myThread2.start();
        try {
            myThread1.join();
            myThread2.join();
        } catch (InterruptedException e) {}
        System.out.println(myPoint);
    }
}