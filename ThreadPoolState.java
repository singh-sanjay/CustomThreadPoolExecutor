import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by sanjaysingh on 7/10/17.
 */
class ThreadPoolState {
    private PoolState poolState;
    private final int corePoolSize;
    private final int maxPoolSize;
    private final long threadTimeoutInMillis;
    private AtomicInteger activeThreadCount;
    private AtomicInteger tasksInFileStorage;

    public ThreadPoolState(int corePoolSize, int maxPoolSize, long threadTimeoutInMillis) {
        this.poolState = PoolState.RUNNING;
        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.threadTimeoutInMillis = threadTimeoutInMillis;
        this.activeThreadCount = new AtomicInteger(0);
        this.tasksInFileStorage = new AtomicInteger(0);
    }

    public int getCorePoolSize() {
        return corePoolSize;
    }

    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    public long getThreadTimeoutInMillis() {
        return threadTimeoutInMillis;
    }

    public AtomicInteger getActiveThreadCount() {
        return activeThreadCount;
    }

    public void incrementThreadCount() {
        activeThreadCount.incrementAndGet();
    }

    public void decrementThreadCount() {
        int threadCount = activeThreadCount.decrementAndGet();
        if (threadCount == 0) {
            this.poolState = PoolState.TERMINATED;
        }
    }

    public AtomicInteger getTasksInFileStorage() {
        return tasksInFileStorage;
    }

    public void incrementStoredTaskCount() {
        tasksInFileStorage.incrementAndGet();
    }

    public void decrementStoredTaskCount() {
        tasksInFileStorage.decrementAndGet();
    }

    public void shutDown() {
        this.poolState = PoolState.SHUTDOWN;
    }

    public void shutDownNow() {
        this.poolState = PoolState.SHUTDOWN_NOW;
    }

    public boolean isShutDown() {
        return poolState != PoolState.RUNNING;
    }

    public boolean isShutDownNow() {
        return poolState == PoolState.SHUTDOWN_NOW;
    }

    public boolean isTerminated() {
        return poolState == PoolState.TERMINATED;
    }

    private enum PoolState {
        RUNNING,
        SHUTDOWN,
        SHUTDOWN_NOW,
        TERMINATED;
    }
}
