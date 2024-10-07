import io.pixelsdb.pixels.scaling.MonitorClient;
import org.junit.Test;

public class TestMonitor {
    @Test
    public void test() {
        MonitorClient client = new MonitorClient(54333);
        client.reportMetric(5);
    }
}
