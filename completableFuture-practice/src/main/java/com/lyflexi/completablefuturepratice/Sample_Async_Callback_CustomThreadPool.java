package com.lyflexi.completablefuturepratice;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.*;

/**
 * @author hasee
 * @version V1.00
 * @time 2025/11/15 20:45
 * @description
 */
@Slf4j
public class Sample_Async_Callback_CustomThreadPool {
    /**
     * 自定义线程池中的线程都是非守护线程, 因此当主线程结束不会对异步任务以及回调函数造成影响
     */
    static final ExecutorService ioPool = new ThreadPoolExecutor(
            4, 8, 60L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(100),
            new ThreadPoolExecutor.CallerRunsPolicy()
    );
    /**
     *
     * @param args
     */
    public static void main(String[] args) {
//        thenApply();
//        thenAccept();
        thenRun();
    }

    /**
     * thenApply回调, 可以接收参数, 返回参数
     */
    private static void thenApply(){
        CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {
            try {
                log.info("异步线程开始执行 {}", Thread.currentThread().getName());
                Thread.sleep(5000);
                log.info("异步线程执行结束 {}", Thread.currentThread().getName());
            } catch (InterruptedException e) {

            }
            return "Hello";
        },ioPool);

        CompletableFuture<Integer> result = future.thenApply(s -> {
            // s 就是上一阶段的结果
            log.info("thenApply callback param : {}, callback thread {}", s, Thread.currentThread().getName());
            return s.length();
        });

        log.info("main ...不会关闭异步任务和回调函数");

    }

    /**
     * thenAccept回调, 智能接收参数, 不能返回参数
     */
    private static void thenAccept(){
        CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {
            try {
                log.info("异步线程开始执行 {}", Thread.currentThread().getName());
                Thread.sleep(5000);
                log.info("异步线程执行结束 {}", Thread.currentThread().getName());
            } catch (InterruptedException e) {

            }
            return "Hello";
        },ioPool);

        future.thenAccept(s -> {
            // s 就是上一阶段的结果
            log.info("thenAccept callback param : {}, callback thread {}", s, Thread.currentThread().getName());
        });

        log.info("main ...不会关闭异步任务和回调函数");

    }

    /**
     * thenRun回调, 不能接收参数, 也不能返回参数
     */
    private static void thenRun(){
        for (int i = 0; i < 2; i++) {
            CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {
                try {
                    log.info("异步线程开始执行 {}", Thread.currentThread().getName());
                    Thread.sleep(5000);
                    log.info("异步线程执行结束 {}", Thread.currentThread().getName());
                } catch (InterruptedException e) {

                }
                return "Hello";
            },ioPool);

            future.thenRun(() -> {
                log.info("callback thread {}",  Thread.currentThread().getName());
            });
        }

        log.info("main ...不会关闭异步任务和回调函数");

    }
}
