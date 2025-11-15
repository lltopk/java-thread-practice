package com.lyflexi.completablefuturepratice;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * @author hasee
 * @version V1.00
 * @time 2025/11/15 20:45
 * @description
 */
@Slf4j
public class Sample_Async_Callback_DefaultThreadPool {
    /**
     * 默认线程池是: ForkJoinPool.commonPool(), 由ForkJoinPool创建出来的线程都是守护线程daemon,
     *
     * 因此当新线程任务执行耗时过长, 主线程是不会等待回调函数执行的, 甚至都不会等待异步任务执行结束, 此时回调函数和异步任务都将失效
     *
     * 除非在主函数结束之前手动get(), 但这样同样阻塞了主线程的结束
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
        });

        CompletableFuture<Integer> result = future.thenApply(s -> {
            // s 就是上一阶段的结果
            log.info("thenApply callback param : {}, callback thread {}", s, Thread.currentThread().getName());
            return s.length();
        });

        log.info("main ...会强行结束异步任务和回调函数, 除非阻塞主线程");
        try {
            result.get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
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
        });

        CompletableFuture<Void> result = future.thenAccept(s -> {
            // s 就是上一阶段的结果
            log.info("thenAccept callback param : {}, callback thread {}", s, Thread.currentThread().getName());
        });

        //CompletableFuture的优势是, 这里不阻塞
        try {
            result.get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * thenRun回调, 不能接收参数, 也不能返回参数
     */
    private static void thenRun(){
        CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {

            try {
                log.info("异步线程开始执行 {}", Thread.currentThread().getName());
                Thread.sleep(5000);
                log.info("异步线程执行结束 {}", Thread.currentThread().getName());
            } catch (InterruptedException e) {

            }
            return "Hello";
        });

        CompletableFuture<Void> result = future.thenRun(() -> {
            log.info("callback thread {}",  Thread.currentThread().getName());
        });

        //CompletableFuture的优势是, 这里不阻塞
        try {
            result.get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
