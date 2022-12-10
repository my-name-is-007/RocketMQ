/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.common;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

public abstract class ServiceThread implements Runnable {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    /** 当 其他对象 调用 本类的shutdown方法时, 需要一定时间让{@link #thread} 执行完, 正确关闭. **/
    private static final long JOIN_TIME = 90 * 1000;

    /** 执行线程: 有此属性, 说明具备了执行能力. **/
    private Thread thread;

    /** Rocket自定义的类: 允许 state 重置为初始值. **/
    protected final CountDownLatch2 waitPoint = new CountDownLatch2(1);

    /** 是否被唤醒过. **/
    protected volatile AtomicBoolean hasNotified = new AtomicBoolean(false);

    /** {@link #thread} 是否为 后台线程. **/
    protected boolean isDaemon = false;

    /** 线程结束的标识. **/
    protected volatile boolean stopped = false;

    /** 线程是否已启动. **/
    private final AtomicBoolean started = new AtomicBoolean(false);

    public ServiceThread() { }

    public abstract String getServiceName();

    public void start() {
        log.info("Try to start service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);
        if (!started.compareAndSet(false, true)) {
            return;
        }
        stopped = false;
        //将自己传入, 执行自己的run方法
        this.thread = new Thread(this, getServiceName());
        this.thread.setDaemon(isDaemon);
        this.thread.start();
    }

    public void shutdown() { this.shutdown(false); }

    public void shutdown(final boolean interrupt) {
        log.info("Try to shutdown service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);

        //设置 started 信号量
        if (!started.compareAndSet(true, false)) { return; }

        //设置 stopped 信号量
        this.stopped = true;
        log.info("shutdown thread " + this.getServiceName() + " interrupt " + interrupt);

        if (hasNotified.compareAndSet(false, true)) {
            //唤醒被阻塞的线程(们)
            waitPoint.countDown();
        }

        try {
            if (interrupt) {
                //中断当前线程
                this.thread.interrupt();
            }

            //当前对象的线程 不是 后台线程 的话: 让它加入当前线程, 让它正确的结束
            long beginTime = System.currentTimeMillis();
            if (!this.thread.isDaemon()) {
                this.thread.join(this.getJointime());
            }
            long elapsedTime = System.currentTimeMillis() - beginTime;
            log.info("join thread " + this.getServiceName() + " elapsed time(ms) " + elapsedTime + " "
                + this.getJointime());
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        }
    }

    /** 标识 stopped 信号量 为 true. **/
    public void makeStop() {
        if (!started.get()) {
            return;
        }
        this.stopped = true;
        log.info("makestop thread " + this.getServiceName());
    }

    /** 设置 hasNotified 为 true, 并调用 {@link #waitPoint} countDown(), 唤醒被阻塞线程. **/
    public void wakeup() {
        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown();
        }
    }

    /** 将 当前线程 阻塞指定时间: 调用 CountDownLatch2#await. **/
    protected void waitForRunning(long interval) {
        //已经唤醒过了: 不做处理.
        if (hasNotified.compareAndSet(true, false)) {
            this.onWaitEnd();
            return;
        }

        //将 state资源 重置为 初始值
        waitPoint.reset();

        try {
            //阻塞 指定时间
            waitPoint.await(interval, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        } finally {
            hasNotified.set(false);
            this.onWaitEnd();
        }
    }

    protected void onWaitEnd() { }
    public boolean isStopped() { return stopped; }
    public boolean isDaemon() { return isDaemon; }
    public void setDaemon(boolean daemon) { isDaemon = daemon; }
    public long getJointime() { return JOIN_TIME; }

    @Deprecated
    public void stop() { this.stop(false); }
    @Deprecated
    public void stop(final boolean interrupt) {
        if (!started.get()) {
            return;
        }
        this.stopped = true;
        log.info("stop thread " + this.getServiceName() + " interrupt " + interrupt);

        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }

        if (interrupt) {
            this.thread.interrupt();
        }
    }
}
