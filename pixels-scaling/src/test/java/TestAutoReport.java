import io.pixelsdb.pixels.common.turbo.MetricsCollector;
import org.junit.Test;

public class TestAutoReport {
    @Test
    public void test() {
        if (MetricsCollector.Instance().isPresent()) {
            System.out.println("reporting...");
            MetricsCollector.Instance().get().report();
        }
    }
}
