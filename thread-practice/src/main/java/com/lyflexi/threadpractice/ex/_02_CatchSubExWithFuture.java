package com.lyflexi.threadpractice.ex;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class _02_CatchSubExWithFuture {
    public static void main(String[] args) {
        ExecutorService executor = Executors.newSingleThreadExecutor();

        Future<Integer> future = executor.submit(() -> {
            // 子线程抛出异常
            throw new RuntimeException("子线程异常");
        });

        try {
            Integer result = future.get();
            System.out.println("子线程结果: " + result);
        } catch (Exception e) {
            Throwable cause = e.getCause();
            System.out.println("捕获到子线程异常: "+cause);
        }

        executor.shutdown();
    }
}
