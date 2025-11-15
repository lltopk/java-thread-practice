package com.lyflexi.completablefuturepratice;

import lombok.extern.slf4j.Slf4j;

/**
 * @author hasee
 * @version V1.00
 * @time 2025/11/15 21:15
 * @description
 */
@Slf4j
public class Sample_New_Thread {
    /**
     * new一个线程默认是非守护线程，因此主线程会等待新线程里面的逻辑执行完才销毁
     * @param args
     */
    public static void main(String[] args) {
        new Thread(() -> {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            log.info("new thread not daemon{}",  Thread.currentThread().getName());
        }).start();

        log.info("main ...");
    }
}
