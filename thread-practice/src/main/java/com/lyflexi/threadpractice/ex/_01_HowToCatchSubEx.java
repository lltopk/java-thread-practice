package com.lyflexi.threadpractice.ex;

public class _01_HowToCatchSubEx {
    public static void main(String[] args) {
        Thread childThread = new Thread(() -> {
            try {
                // 子线程抛出异常
                int result = 10 / 0;
            } catch (ArithmeticException e) {
                System.out.println("子线程异常: "+e);
                throw e;
            }
        });

        try {
            // 主线程等待子线程执行完成
            childThread.start();
        } catch (Exception e) {
            System.out.println("主线程捕获到子线程异常: "+e);
        }

        System.out.println("主线程执行");
    }
}
