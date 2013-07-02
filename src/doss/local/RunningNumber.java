package doss.local;

import java.util.concurrent.atomic.AtomicLong;

public class RunningNumber {

    private final AtomicLong number = new AtomicLong();

    public long next() {
        return number.incrementAndGet();
    }
    
}
