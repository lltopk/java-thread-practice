ForkJoinPool由静态代码块初始化, 使用的默认线程池工厂是DefaultForkJoinWorkerThreadFactory
```JAVA
    static final ForkJoinPool common;
    static {
    ...
        @SuppressWarnings("removal")
        ForkJoinPool tmp = AccessController.doPrivileged(new PrivilegedAction<>() {
            public ForkJoinPool run() {
                return new ForkJoinPool((byte)0); }});
        common = tmp;
    ...
    }
```
ForkJoinPool使用的构造函数如下, 如果没有给Java启动参数-D指定threadFactory, 则使用DefaultCommonPoolForkJoinWorkerThreadFactory
```java
    private ForkJoinPool(byte forCommonPoolOnly) {
        int parallelism = Math.max(1, Runtime.getRuntime().availableProcessors() - 1);
        ForkJoinWorkerThreadFactory fac = null;
        UncaughtExceptionHandler handler = null;
        try {  // ignore exceptions in accessing/parsing properties
            fac = (ForkJoinWorkerThreadFactory) newInstanceFromSystemProperty(
                "java.util.concurrent.ForkJoinPool.common.threadFactory");
            handler = (UncaughtExceptionHandler) newInstanceFromSystemProperty(
                "java.util.concurrent.ForkJoinPool.common.exceptionHandler");
            String pp = System.getProperty
                ("java.util.concurrent.ForkJoinPool.common.parallelism");
            if (pp != null)
                parallelism = Integer.parseInt(pp);
        } catch (Exception ignore) {
        }
        this.ueh = handler;
        this.keepAlive = DEFAULT_KEEPALIVE;
        this.saturate = null;
        this.workerNamePrefix = null;
        int p = Math.min(Math.max(parallelism, 0), MAX_CAP), size;
        this.mode = p;
        if (p > 0) {
            size = 1 << (33 - Integer.numberOfLeadingZeros(p - 1));
            this.bounds = ((1 - p) & SMASK) | (COMMON_MAX_SPARES << SWIDTH);
            this.ctl = ((((long)(-p) << TC_SHIFT) & TC_MASK) |
                        (((long)(-p) << RC_SHIFT) & RC_MASK));
        } else {  // zero min, max, spare counts, 1 slot
            size = 1;
            this.bounds = 0;
            this.ctl = 0L;
        }
        this.factory = (fac != null) ? fac :
            new DefaultCommonPoolForkJoinWorkerThreadFactory();
        this.queues = new WorkQueue[size];
        this.registrationLock = new ReentrantLock();
    }
```

DefaultCommonPoolForkJoinWorkerThreadFactory创建的线程是ForkJoinWorkerThread
```java
    static final class DefaultCommonPoolForkJoinWorkerThreadFactory
        implements ForkJoinWorkerThreadFactory {
        @SuppressWarnings("removal")
        private static final AccessControlContext ACC = contextWithPermissions(
            modifyThreadPermission,
            new RuntimePermission("enableContextClassLoaderOverride"),
            new RuntimePermission("modifyThreadGroup"),
            new RuntimePermission("getClassLoader"),
            new RuntimePermission("setContextClassLoader"));

        @SuppressWarnings("removal")
        public final ForkJoinWorkerThread newThread(ForkJoinPool pool) {
            return AccessController.doPrivileged(
                 new PrivilegedAction<>() {
                     public ForkJoinWorkerThread run() {
                         return System.getSecurityManager() == null ?
                             new ForkJoinWorkerThread(null, pool, true, true):
                             new ForkJoinWorkerThread.
                             InnocuousForkJoinWorkerThread(pool); }},
                 ACC);
        }
    }
```
ForkJoinWorkerThread默认设置守护线程
```java
    ForkJoinWorkerThread(ThreadGroup group, ForkJoinPool pool,
                         boolean useSystemClassLoader, boolean isInnocuous) {
    super(group, null, pool.nextWorkerThreadName(), 0L);
    UncaughtExceptionHandler handler = (this.pool = pool).ueh;
    this.workQueue = new ForkJoinPool.WorkQueue(this, isInnocuous);
    super.setDaemon(true);
    if (handler != null)
        super.setUncaughtExceptionHandler(handler);
    if (useSystemClassLoader)
        super.setContextClassLoader(ClassLoader.getSystemClassLoader());
}
```

其中, DefaultCommonPoolForkJoinWorkerThreadFactory#newThread是懒加载
```java
    private boolean createWorker() {
        ForkJoinWorkerThreadFactory fac = factory;
        Throwable ex = null;
        ForkJoinWorkerThread wt = null;
        try {
            if (fac != null && (wt = fac.newThread(this)) != null) {
                wt.start();
                return true;
            }
        } catch (Throwable rex) {
            ex = rex;
        }
        deregisterWorker(wt, ex);
        return false;
    }
/*
 * Tries to create or release a worker if too few are running.
 */
final void signalWork() {
    for (long c = ctl; c < 0L;) {
        int sp, i; WorkQueue[] qs; WorkQueue v;
        if ((sp = (int)c & ~UNSIGNALLED) == 0) {  // no idle workers
            if ((c & ADD_WORKER) == 0L)           // enough total workers
                break;
            if (c == (c = compareAndExchangeCtl(
                    c, ((RC_MASK & (c + RC_UNIT)) |
                            (TC_MASK & (c + TC_UNIT)))))) {
                createWorker();
                break;
            }
        }
        else if ((qs = queues) == null)
            break;                                // unstarted/terminated
        else if (qs.length <= (i = sp & SMASK))
            break;                                // terminated
        else if ((v = qs[i]) == null)
            break;                                // terminating
        else {
            long nc = (v.stackPred & SP_MASK) | (UC_MASK & (c + RC_UNIT));
            Thread vt = v.owner;
            if (c == (c = compareAndExchangeCtl(c, nc))) {
                v.phase = sp;
                LockSupport.unpark(vt);           // release idle worker
                break;
            }
        }
    }
}

final void externalPush(ForkJoinTask<?> task) {
    WorkQueue q;
    if ((q = submissionQueue()) == null)
        throw new RejectedExecutionException(); // shutdown or disabled
    else if (q.lockedPush(task))
        signalWork();
}

private <T> ForkJoinTask<T> externalSubmit(ForkJoinTask<T> task) {
    Thread t; ForkJoinWorkerThread wt; WorkQueue q;
    if (task == null)
        throw new NullPointerException();
    if (((t = Thread.currentThread()) instanceof ForkJoinWorkerThread) &&
            (q = (wt = (ForkJoinWorkerThread)t).workQueue) != null &&
            wt.pool == this)
        q.push(task, this);
    else
        externalPush(task);
    return task;
}
public void execute(ForkJoinTask<?> task) {
    externalSubmit(task);
}
```