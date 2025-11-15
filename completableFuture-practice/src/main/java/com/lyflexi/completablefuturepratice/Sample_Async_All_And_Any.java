package com.lyflexi.completablefuturepratice;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author hasee
 * @version V1.00
 * @time 2025/11/15 22:24
 * @description
 */
public class Sample_Async_All_And_Any {
    public static void main(String[] args) {
        System.out.println("开始执行异步任务...");

        // 创建三个异步任务
        CompletableFuture<String> task1 = simulateAsyncTask("Task-1", 2000); // 2秒
        CompletableFuture<String> task2 = simulateAsyncTask("Task-2", 3000); // 3秒
        CompletableFuture<String> task3 = simulateAsyncTask("Task-3", 1000); // 1秒

        // ========== allOf()：等待所有任务完成 ==========
        System.out.println("\n--- 使用 allOf() ---");
        CompletableFuture<Void> allDone = CompletableFuture.allOf(task1, task2, task3);
        allDone.join(); // 阻塞直到全部完成
        System.out.println("所有任务已完成！");
        System.out.println("结果: " + task1.join() + ", " + task2.join() + ", " + task3.join());

        // ========== anyOf()：任一任务完成即返回 ==========
        System.out.println("\n--- 使用 anyOf() ---");
        CompletableFuture<Object> firstDone = CompletableFuture.anyOf(
                simulateAsyncTask("Fast-Task", 500),
                simulateAsyncTask("Slow-Task", 2500),
                simulateAsyncTask("Medium-Task", 1500)
        );

        Object result = firstDone.join(); // 获取最快完成的任务结果
        System.out.println("最快完成的任务结果: " + result);
    }

    // 模拟异步任务：延迟后返回字符串
    private static CompletableFuture<String> simulateAsyncTask(String name, long delayMs) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                TimeUnit.MILLISECONDS.sleep(delayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            return name + " 完成（耗时 " + delayMs + "ms）";
        });
    }
}
