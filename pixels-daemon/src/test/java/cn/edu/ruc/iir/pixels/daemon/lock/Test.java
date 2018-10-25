package cn.edu.ruc.iir.pixels.daemon.lock;

public class Test {
    static int n = 500;

    public static void secskill() {
        System.out.println(--n);
    }

    public static void main(String[] args) {

        Runnable runnable = new Runnable() {
            public void run() {
                DistributedLock lock = null;
                try {
                    lock = new DistributedLock("dbiir02:2181", "test1");
                    lock.lock();
                    secskill();
                    System.out.println(Thread.currentThread().getName() + "正在运行");
                } finally {
                    if (lock != null) {
                        lock.unlock();
                    }
                }
            }
        };

        for (int i = 0; i < 10; i++) {
            Thread t = new Thread(runnable);
            t.start();
        }
    }
}
