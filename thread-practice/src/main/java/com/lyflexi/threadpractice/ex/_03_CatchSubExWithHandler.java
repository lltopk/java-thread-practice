package com.lyflexi.threadpractice.ex;

public class _03_CatchSubExWithHandler {
    public static void main(String[] args) {
        Thread thread = new Thread(() -> {
            throw new RuntimeException("这是一个未捕获异常");
        });

        // 设置自定义的未捕获异常处理器
        thread.setUncaughtExceptionHandler(new HollisUncaughtExceptionHandler());

        thread.start();
    }

    static class HollisUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
            System.out.println("线程 " + t.getName() + " 抛出未捕获异常：" + e.getMessage());
            // 在这里可以执行自定义的异常处理逻辑
        }
    }
}
