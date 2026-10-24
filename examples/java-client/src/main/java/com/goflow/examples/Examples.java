package com.goflow.examples;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Supplier;

/** Small helpers shared by the examples. */
final class Examples {

    private Examples() {
    }

    static byte[] resource(String name) {
        try (InputStream in = Examples.class.getResourceAsStream("/" + name)) {
            if (in == null) {
                throw new IllegalStateException("missing resource " + name);
            }
            return in.readAllBytes();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Polls until the supplier returns a value, or fails after the timeout. */
    static <T> T waitFor(String what, Duration timeout, Supplier<Optional<T>> probe) {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            Optional<T> v = probe.get();
            if (v.isPresent()) {
                return v.get();
            }
            sleep(300);
        }
        throw new IllegalStateException("timed out waiting for " + what);
    }

    static void check(boolean ok, String message) {
        if (!ok) {
            throw new IllegalStateException("CHECK FAILED: " + message);
        }
        System.out.println("  ✓ " + message);
    }

    static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
