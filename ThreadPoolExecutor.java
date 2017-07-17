import java.io.*;
import java.nio.channels.FileChannel;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by sanjaysingh on 7/10/17.
 */
public class ThreadPoolExecutor {
    private ThreadPoolState poolState;
    private BlockingQueue<Runnable> waitingTaskQueue;
    private Queue<WorkerThread> workerThreads;
    private final FileChannel fileChannel;
    private final String fileName;

    public ThreadPoolExecutor(int corePoolSize, int maxPoolSize, int maxQueueSize,
                              long timeOutInMilliseconds, String fileName )
            throws IOException {
        this.poolState = new ThreadPoolState(corePoolSize, maxPoolSize, timeOutInMilliseconds);
        this.waitingTaskQueue = new ArrayBlockingQueue<>(maxQueueSize);
        this.workerThreads = new ArrayBlockingQueue<>(maxPoolSize);
        this.fileChannel = new RandomAccessFile(fileName, "rw").getChannel();
        this.fileName = fileName;

        // init core pool
        for (int i=0; i<corePoolSize; i++) {
            WorkerThread wt = new WorkerThread(this.waitingTaskQueue,
                    this.poolState, this.fileChannel, this.fileName);
            workerThreads.add(wt);
        }
    }

    public void execute(Runnable task) {
        synchronized (poolState) {
            //do not accept any tasks when pool is shutting-down/terminated
            if (poolState.isShutDown()) {
                return;
            }
            WorkerThread worker;
            //core threads available
            if (!workerThreads.isEmpty()) {
                worker = workerThreads.remove();
                worker.submit(task);
                worker.start();
                return;
            }
            // create threads till maxPoolSize
            if (poolState.getActiveThreadCount().get() < poolState.getMaxPoolSize()) {
                worker = new WorkerThread(this.waitingTaskQueue, this.poolState,
                        this.fileChannel, this.fileName);
                worker.submit(task);
                worker.start();
                return;
            }
            // submit task to queue
            if(!waitingTaskQueue.offer(task)) {
                // queue is full
                writeToFile(task);
            }
        }
    }

    private void writeToFile(Runnable task) {
        synchronized (fileChannel) {
            try (
                    FileOutputStream fos = new FileOutputStream(fileName, true);
                    ObjectOutputStream oos = new ObjectOutputStream(fos)
                    ){
                poolState.incrementStoredTaskCount();
                oos.writeObject(task);
            } catch (IOException e) {
                // File IO Exception
                e.printStackTrace();
            }
        }
    }

    public void shutDown() {
        synchronized (poolState) {
            poolState.shutDown();
        }
    }

    public void shutDownNow() {
        synchronized (poolState) {
            poolState.shutDownNow();
        }
    }
}
