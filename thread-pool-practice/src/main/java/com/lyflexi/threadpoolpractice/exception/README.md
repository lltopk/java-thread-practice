
ThreadPoolTask线程池两种提交任务方式分别是，execute  submit

区别就不多说了，execute接收Runnable任务但不提供返回值，submit接收Callable任务提供返回值

本文目的是观察二者对于未捕获异常的处置方式，所谓未捕获异常指的是没被try-catch捕获的运行时异常

```java

@Slf4j
public class ThreadPoolTaskExceptionSample {
    static ExecutorService pool = Executors.newFixedThreadPool(1);

    /**
     * 控制台默认没有打印异常信息: submit不打印异常信息，只有execute会打印异常信息！，
     * @param args
     */
    public static void main(String[] args) {
testExecute(pool);
    }

    /**
     * @description: execute会打印异常信息！，
     * @author: hmly
     * @date: 2025/7/13 14:38
     * @param: [pool]
     * @return: void
     **/
    private static void testExecute(ExecutorService pool) {
pool.execute(() -> {
            log.debug("task");
            int i = 1 / 0;

        });
    }

    /**
     * @description: submit不打印异常信息！，
     * @author: hmly
     * @date: 2025/7/13 14:38
     * @param: [pool]
     * @return: void
     **/
    private static void testSubmit(ExecutorService pool) {
pool.submit(() -> {
            log.debug("task");
            int i = 1 / 0;

        });
    }
}
```

## 方式一显示的try-catch

显示的try-catch

```java
@Slf4j
public class ThreadPoolTaskExceptionSample2 {
    /**
     * 必须catch之后手动打印异常信息，控制台才会打印异常信息
     * @param args
     */
    public static void main(String[] args) {
        ExecutorService pool = Executors.newFixedThreadPool(1);
pool.submit(() -> {
            try {
                log.debug("task");
                int i = 1 / 0;
            } catch (Exception e) {
e.printStackTrace();
            }
        });
    }
}
```

## 方式二手动get()

通过get方法，因为get方法不仅会存储计算结果，也会存储计算异常

```java
    /** The result to return or exception to throw from get() */
    private Object outcome; // non-volatile, protected by state reads/writes
```

示例如下

```java
@Slf4j
public class ThreadPoolTaskExceptionSample3 {
    /**
     * 利用Callable和Future，当任务执行异常时候，get会取到异常信息
     * @param args
     */
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(1);
        Future<Boolean> f = pool.submit(() -> {

            log.debug("task");
            int i = 1 / 0;
            return true;

        });

        log.debug("task:{}", f.get());
    }
}
```

## 为什么FutureTask吃掉了异常

AbstractExecutorService#submit

```java
    /**
     * @throws RejectedExecutionException {@inheritDoc}
     * @throws NullPointerException       {@inheritDoc}
     */
    public <T> Future<T> submit(Callable<T> task) {
        if (task == null) throw new NullPointerException();
        RunnableFuture<T> ftask = newTaskFor(task);
        execute(ftask);
        return ftask;
    }
```

可以看到，submit同样是调用了execute，只不过将execute参数包装成了RunnableFuture，RunnableFuture继承了Runnable，因此execute接收RunnableFuture合情合理

```java
    public void execute(Runnable command) {
        if (command == null)
            throw new NullPointerException();
        /*
         * Proceed in 3 steps:
         *
         * 1. If fewer than corePoolSize threads are running, try to
         * start a new thread with the given command as its first
         * task.  The call to addWorker atomically checks runState and
         * workerCount, and so prevents false alarms that would add
         * threads when it shouldn't, by returning false.
         *
         * 2. If a task can be successfully queued, then we still need
         * to double-check whether we should have added a thread
         * (because existing ones died since last checking) or that
         * the pool shut down since entry into this method. So we
         * recheck state and if necessary roll back the enqueuing if
         * stopped, or start a new thread if there are none.
         *
         * 3. If we cannot queue task, then we try to add a new
         * thread.  If it fails, we know we are shut down or saturated
         * and so reject the task.
         */
        int c = ctl.get();
        if (workerCountOf(c) < corePoolSize) {
            if (addWorker(command, true))
                return;
            c = ctl.get();
        }
        if (isRunning(c) && workQueue.offer(command)) {
            int recheck = ctl.get();
            if (! isRunning(recheck) && remove(command))
                reject(command);
            else if (workerCountOf(recheck) == 0)
                addWorker(null, false);
        }
        else if (!addWorker(command, false))
            reject(command);
    }
```

execute做的事情就是将任务加入队列addWorker

```java
    private boolean addWorker(Runnable firstTask, boolean core) {
        retry:
        for (;;) {
            int c = ctl.get();
            int rs = runStateOf(c);

            // Check if queue empty only if necessary.
            if (rs >= SHUTDOWN &&
                ! (rs == SHUTDOWN &&
                   firstTask == null &&
                   ! workQueue.isEmpty()))
                return false;

            for (;;) {
                int wc = workerCountOf(c);
                if (wc >= CAPACITY ||
                    wc >= (core ? corePoolSize : maximumPoolSize))
                    return false;
                if (compareAndIncrementWorkerCount(c))
                    break retry;
                c = ctl.get();  // Re-read ctl
                if (runStateOf(c) != rs)
                    continue retry;
                // else CAS failed due to workerCount change; retry inner loop
            }
        }

        boolean workerStarted = false;
        boolean workerAdded = false;
        Worker w = null;
        try {
            w = new Worker(firstTask);
            final Thread t = w.thread;
            if (t != null) {
                final ReentrantLock mainLock = this.mainLock;
                mainLock.lock();
                try {
                    // Recheck while holding lock.
                    // Back out on ThreadFactory failure or if
                    // shut down before lock acquired.
                    int rs = runStateOf(ctl.get());

                    if (rs < SHUTDOWN ||
                        (rs == SHUTDOWN && firstTask == null)) {
                        if (t.isAlive()) // precheck that t is startable
                            throw new IllegalThreadStateException();
                        workers.add(w);
                        int s = workers.size();
                        if (s > largestPoolSize)
                            largestPoolSize = s;
                        workerAdded = true;
                    }
                } finally {
                    mainLock.unlock();
                }
                if (workerAdded) {
                    t.start();
                    workerStarted = true;
                }
            }
        } finally {
            if (! workerStarted)
                addWorkerFailed(w);
        }
        return workerStarted;
    }
```

大概就是为任务创建工作线程Worker，因此我们看工作线程Worker的消费逻辑, 来看内部类Worker

```java
    private final class Worker
        extends AbstractQueuedSynchronizer
        implements Runnable {
    /**
     * This class will never be serialized, but we provide a
     * serialVersionUID to suppress a javac warning.
     */
    private static final long serialVersionUID = 6138294804551838833L;

    /** Thread this worker is running in.  Null if factory fails. */
    final Thread thread;
    /** Initial task to run.  Possibly null. */
    Runnable firstTask;
    /** Per-thread task counter */
    volatile long completedTasks;

    /**
     * Creates with given first task and thread from ThreadFactory.
     * @param firstTask the first task (null if none)
     */
    Worker(Runnable firstTask) {
        setState(-1); // inhibit interrupts until runWorker
        this.firstTask = firstTask;
        this.thread = getThreadFactory().newThread(this);
    }

    /** Delegates main run loop to outer runWorker  */
    public void run() {
        runWorker(this);
    }
    ......
}
```

Delegates main run loop to outer runWorker

```java
    final void runWorker(Worker w) {
        Thread wt = Thread.currentThread();
        Runnable task = w.firstTask;
        w.firstTask = null;
        w.unlock(); // allow interrupts
        boolean completedAbruptly = true;
        try {
            while (task != null || (task = getTask()) != null) {
                w.lock();
                // If pool is stopping, ensure thread is interrupted;
                // if not, ensure thread is not interrupted.  This
                // requires a recheck in second case to deal with
                // shutdownNow race while clearing interrupt
                if ((runStateAtLeast(ctl.get(), STOP) ||
                     (Thread.interrupted() &&
                      runStateAtLeast(ctl.get(), STOP))) &&
                    !wt.isInterrupted())
                    wt.interrupt();
                try {
                    beforeExecute(wt, task);
                    Throwable thrown = null;
                    try {
                        task.run();
                    } catch (RuntimeException x) {
                        thrown = x; throw x;
                    } catch (Error x) {
                        thrown = x; throw x;
                    } catch (Throwable x) {
                        thrown = x; throw new Error(x);
                    } finally {
                        afterExecute(task, thrown);
                    }
                } finally {
                    task = null;
                    w.completedTasks++;
                    w.unlock();
                }
            }
            completedAbruptly = false;
        } finally {
            processWorkerExit(w, completedAbruptly);
        }
    }
```

可以看到，如果task.run();有抛出异常，比如execute提交的runnable存在异常，那么runWorker一定可以捕获到抛给用户，

既然runWorker没有捕获到futuretask的异常，那么一定是futuretask的run方法吃掉了异常

下面来看FutureTask#run, 确实

```java
    public void run() {
        if (state != NEW ||
            !UNSAFE.compareAndSwapObject(this, runnerOffset,
                                         null, Thread.currentThread()))
            return;
        try {
            Callable<V> c = callable;
            if (c != null && state == NEW) {
                V result;
                boolean ran;
                try {
                    result = c.call();
                    ran = true;
                } catch (Throwable ex) {
                    result = null;
                    ran = false;
                    setException(ex);
                }
                if (ran)
                    set(result);
            }
        } finally {
            // runner must be non-null until state is settled to
            // prevent concurrent calls to run()
            runner = null;
            // state must be re-read after nulling runner to prevent
            // leaked interrupts
            int s = state;
            if (s >= INTERRUPTING)
                handlePossibleCancellationInterrupt(s);
        }
    }
```

当futuretask发生异常，会自己吃掉异常 ，并通过setException(ex);设置给内部的outcome，用户只能通过get的方式获得异常

```java
    protected void setException(Throwable t) {
        if (UNSAFE.compareAndSwapInt(this, stateOffset, NEW, COMPLETING)) {
            outcome = t;
            UNSAFE.putOrderedInt(this, stateOffset, EXCEPTIONAL); // final state
            finishCompletion();
        }
    }
```

## 如何把FutureTask异常捕获和获得的权力交给用户

真相大白了, 上面的两种获得方式

- 用户手动的try-catch, 相当于提前拿到了异常, 避免FutureTask.run的时候被jdk捕获
- 用户手动get, 相当于是FutureTask.run的时候被jdk捕获之后, 存在了outcome中, 这时用户再从outcome中获取


接下来要做的是，如何把FutureTask异常捕获和获得的权力交给用户？ 当有未捕获异常发生的时候自动抛给用户

ThreadPoolExecutor#runWorker中有个未实现的抽象方法afterExecute(task, thrown);

```java
    final void runWorker(Worker w) {
        Thread wt = Thread.currentThread();
        Runnable task = w.firstTask;
        w.firstTask = null;
        w.unlock(); // allow interrupts
        boolean completedAbruptly = true;
        try {
            while (task != null || (task = getTask()) != null) {
                w.lock();
                // If pool is stopping, ensure thread is interrupted;
                // if not, ensure thread is not interrupted.  This
                // requires a recheck in second case to deal with
                // shutdownNow race while clearing interrupt
                if ((runStateAtLeast(ctl.get(), STOP) ||
                     (Thread.interrupted() &&
                      runStateAtLeast(ctl.get(), STOP))) &&
                    !wt.isInterrupted())
                    wt.interrupt();
                try {
                    beforeExecute(wt, task);
                    Throwable thrown = null;
                    try {
                        task.run();
                    } catch (RuntimeException x) {
                        thrown = x; throw x;
                    } catch (Error x) {
                        thrown = x; throw x;
                    } catch (Throwable x) {
                        thrown = x; throw new Error(x);
                    } finally {
                        afterExecute(task, thrown);
                    }
                } finally {
                    task = null;
                    w.completedTasks++;
                    w.unlock();
                }
            }
            completedAbruptly = false;
        } finally {
            processWorkerExit(w, completedAbruptly);
        }
    }
```

由于afterExecute(task, thrown);位于finally，因此一定会被执行，所以解决方案就是重写afterExecute(task, thrown);

```java
    static class ExecutorServiceWithUncatchException extends ThreadPoolExecutor {
        /**
         * @description: 父类ThreadPoolExecutor没有无参构造，因此你要继承ThreadPoolExecutor就也不能有无参构造，
         *
         * 方式是提供有参构造覆盖无参构造
         * @author: hmly
         * @date: 2025/7/13 15:18
         * @param: [corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue]
         * @return:
         **/
        public ExecutorServiceWithUncatchException(int corePoolSize,
                                  int maximumPoolSize,
                                  long keepAliveTime,
                                  TimeUnit unit,
                                  BlockingQueue<Runnable> workQueue,
                                  ThreadFactory threadFactory,
                                  RejectedExecutionHandler handler) {
            super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue,threadFactory,handler);
        }
        public ExecutorServiceWithUncatchException(
                int corePoolSize,
                int maximumPoolSize,
                long keepAliveTime,
                TimeUnit unit,
                BlockingQueue<Runnable> workQueue) {
            super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
        }


        //重写afterExecute方法
        @Override
        protected void afterExecute(Runnable r, Throwable t) {
            //这个是execute提交的时候
            if (t != null) {
                System.out.println("afterExecute里面获取到execute提交的异常信息，处理异常" + t.getMessage());
            }
            //如果r的实际类型是FutureTask 那么是submit提交的，所以可以在里面get到异常
            if (r instanceof FutureTask) {
                try {
                    Future<?> future = (Future<?>) r;
                    //get获取异常
                    future.get();

                } catch (Exception e) {
                    System.out.println("afterExecute里面获取到submit提交的异常信息，处理异常" + e);
                }
            }
        }
    }
```

后续就是用我们重写的线程池ExecutorServiceWithUncatchException，代替原生的ThreadPoolExecutor
