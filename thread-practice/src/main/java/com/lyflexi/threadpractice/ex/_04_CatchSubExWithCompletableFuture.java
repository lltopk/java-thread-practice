package com.lyflexi.threadpractice.ex;

import java.util.concurrent.CompletableFuture;

public class _04_CatchSubExWithCompletableFuture {
    public static void main(String[] args) {
        CompletableFuture<Integer> future = CompletableFuture.supplyAsync(() -> {
            // 子线程抛出异常
            throw new RuntimeException("子线程异常");
        });

        future.handle((result, exception) -> {
            if (exception != null) {
                System.out.println("捕获到子线程异常: " + exception.getMessage());
            } else {
                System.out.println("子线程结果: " + result);
            }
            return null;
        });
    }
}
