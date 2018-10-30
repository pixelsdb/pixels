package cn.edu.ruc.iir.pixels.load.pc;

import org.apache.hadoop.fs.Path;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.load.pc
 * @ClassName: PixelsProducer
 * @Description: souce -> BlockingQueue
 * @author: tao
 * @date: Create in 2018-10-30 12:57
 **/
public class PixelsProducer {
    private static PixelsProducer instance = new PixelsProducer();

    public PixelsProducer() {
    }

    public static PixelsProducer getInstance() {
        return instance;
    }

    public void startProducer(BlockingQueue<Path> queue) {
    }

    class Producer extends Thread {

        private volatile boolean isRunning = true;
        private BlockingQueue<Path> queue;
        Path data = null;

        public Producer(BlockingQueue<Path> queue) {
            this.queue = queue;
        }

        @Override
        public void run() {
            System.out.println("start producer thread！");
            try {
                while (isRunning) {
                    System.out.println("begin to generate data...");

                    System.out.println("add：" + data + "into queue...");
                    if (!queue.offer(data, 2, TimeUnit.SECONDS)) {
                        System.out.println("add error：" + data);
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
                Thread.currentThread().interrupt();
            } finally {
                System.out.println("Exit producer thread！");
            }
        }

        public void stopProducer() {
            isRunning = false;
        }
    }

}
