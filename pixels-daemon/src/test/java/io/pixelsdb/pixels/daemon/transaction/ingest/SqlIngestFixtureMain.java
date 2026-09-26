package io.pixelsdb.pixels.daemon.transaction.ingest;

import io.pixelsdb.pixels.retina.RGVisibility;

import java.nio.file.*;

/** Separate backend process: connector dependencies must not contaminate Trino's HTTP runtime. */
public final class SqlIngestFixtureMain {
    private static final long STATUS_EXPORT_INTERVAL_MILLIS = 50L;

    public static void main(String[] args) {
        int exitCode = 0;
        try {
            run(args);
        } catch (Throwable failure) {
            failure.printStackTrace(System.err);
            exitCode = 1;
        }
        // The fixture closes its resources before terminating process-wide client executors.
        System.exit(exitCode);
    }

    private static void run(String[] args) throws Exception {
        Path control = Paths.get(args[0]);
        Files.createDirectories(control);
        verifyNativeVisibility();
        try (SqlIngestFixture fixture = new SqlIngestFixture()) {
            fixture.exportConfiguration(control.resolve("pixels.properties"));
            fixture.exportStatus(control.resolve("status.properties"));
            Files.write(control.resolve("ready"), new byte[0]);
            System.out.println("PIXELS_SQL_FIXTURE_READY " + fixture.root);
            while (!Files.exists(control.resolve("stop"))) {
                fixture.exportStatus(control.resolve("status.properties"));
                Thread.sleep(STATUS_EXPORT_INTERVAL_MILLIS);
            }
            fixture.exportStatus(control.resolve("status.properties"));
        }
        System.out.println("PIXELS_SQL_FIXTURE_STOPPED");
    }

    private static void verifyNativeVisibility() {
        long mask = 1L << 31;
        for (int iteration = 0; iteration < 1000; iteration++) {
            try (RGVisibility visibility = new RGVisibility(64, 0, null)) {
                visibility.deleteRecord(31, 1);
                if ((visibility.getVisibilityBitmap(0)[0] & mask) != 0) {
                    throw new AssertionError("Native visibility exposed a future deletion");
                }
                if ((visibility.getVisibilityBitmap(1)[0] & mask) == 0) {
                    throw new AssertionError("Native visibility lost an installed deletion");
                }
            }
        }
        System.out.println("PIXELS_NATIVE_VISIBILITY_PASS iterations=1000");
    }
}
