/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tomcat.util.net;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;

/**
 * 线程安全的非阻塞selector池
 * Thread safe non blocking selector pool
 * @version 1.0
 * @since 6.0
 */

public class NioSelectorPool {
    //如果是共享Selector，那么就是用NioBlockingSelector，否则使用selector队列。
    public NioSelectorPool() {
    }

    private static final Log log = LogFactory.getLog(NioSelectorPool.class);

    protected static final boolean SHARED =
        Boolean.parseBoolean(System.getProperty("org.apache.tomcat.util.net.NioSelectorShared", "true"));
    //里面封装了一个Selector，封装的其实就是下面这个Selector。
    protected NioBlockingSelector blockingSelector;
    //还有一个共享的Selector
    protected volatile Selector SHARED_SELECTOR;

    protected int maxSelectors = 200;//最大selector个数
    protected long sharedSelectorTimeout = 30000;
    protected int maxSpareSelectors = -1;//最大的备用selector个数
    protected boolean enabled = true;
    protected AtomicInteger active = new AtomicInteger(0);//记录活动的selector个数
    protected AtomicInteger spare = new AtomicInteger(0);//记录备用的selector个数
    //selectors队列
    protected ConcurrentLinkedQueue<Selector> selectors =
            new ConcurrentLinkedQueue<>();
    /*
    这不是著名的双重检查机制？？？因为SHARED_SELECTOR是volatile修饰的，所以没有问题的哈
     */
    protected Selector getSharedSelector() throws IOException {
        if (SHARED && SHARED_SELECTOR == null) {
            synchronized ( NioSelectorPool.class ) {
                if ( SHARED_SELECTOR == null )  {
                    SHARED_SELECTOR = Selector.open();
                    log.info("Using a shared selector for servlet write/read");
                }
            }
        }
        return  SHARED_SELECTOR;
    }
    //从Pool中取Selector
    public Selector get() throws IOException{
        if ( SHARED ) {//如果处于共享selector状态，直接返回SHARD_SELECTOR.
            return getSharedSelector();
        }
        //如果活动selector个数增加导致大于maxSelectors直接返回null， 这里不大于的话会对active+1，下面的代码会取selector
        if ( (!enabled) || active.incrementAndGet() >= maxSelectors ) {
            if ( enabled ) active.decrementAndGet();
            return null;
        }
        Selector s = null;
        try {//从selector队列中取出一个selector
            s = selectors.size()>0?selectors.poll():null;
            if (s == null) {
                s = Selector.open();//没有的话新创建一个selector
            }
            else spare.decrementAndGet();

        }catch (NoSuchElementException x ) {
            try {
                s = Selector.open();
            } catch (IOException iox) {
            }
        } finally {
            if ( s == null ) active.decrementAndGet();//we were unable to find a selector，回退active个数
        }
        return s;
    }


        //向Pool中放入Selector
    public void put(Selector s) throws IOException {
        if ( SHARED ) return;
        if ( enabled ) active.decrementAndGet();
        //用完的Selector会放会Pool中
        if ( enabled && (maxSpareSelectors==-1 || spare.get() < Math.min(maxSpareSelectors,maxSelectors)) ) {
            spare.incrementAndGet();
            selectors.offer(s);
        }
        else s.close();
    }
    //关闭SelectorPool
    public void close() throws IOException {
        enabled = false;
        Selector s;
        while ( (s = selectors.poll()) != null ) s.close();
        spare.set(0);
        active.set(0);
        if (blockingSelector!=null) {
            blockingSelector.close();
        }
        if ( SHARED && getSharedSelector()!=null ) {
            getSharedSelector().close();
            SHARED_SELECTOR = null;
        }
    }
    //打开Selector Pool
    public void open() throws IOException {
        enabled = true;
        getSharedSelector();//承担了sharedSelector的初始化和get方法。
        if (SHARED) {
            blockingSelector = new NioBlockingSelector();
            blockingSelector.open(getSharedSelector());
        }

    }

    /**
     * Performs a write using the bytebuffer for data to be written and a
     * selector to block (if blocking is requested). If the
     * <code>selector</code> parameter is null, and blocking is requested then
     * it will perform a busy write that could take up a lot of CPU cycles.
     *
     * @param buf           The buffer containing the data, we will write as long as <code>(buf.hasRemaining()==true)</code>
     * @param socket        The socket to write data to
     * @param selector      The selector to use for blocking, if null then a busy write will be initiated
     * @param writeTimeout  The timeout for this write operation in milliseconds, -1 means no timeout
     * @param block         <code>true</code> to perform a blocking write
     *                      otherwise a non-blocking write will be performed
     * @return int - returns the number of bytes written
     * @throws EOFException if write returns -1
     * @throws SocketTimeoutException if the write times out
     * @throws IOException if an IO Exception occurs in the underlying socket logic
     */
    public int write(ByteBuffer buf, NioChannel socket, Selector selector,
                     long writeTimeout, boolean block) throws IOException {
        if ( SHARED && block ) {//首先判断是否阻塞写
            return blockingSelector.write(buf,socket,writeTimeout);
        }
        SelectionKey key = null;
        int written = 0;
        boolean timedout = false;
        int keycount = 1; //assume we can write
        long time = System.currentTimeMillis(); //start the timeout timer
        //如果可以无阻碍的顺利写就直接写到socket，
        //如果写入操作没有准备好返回写了0个字节，在Selector上注册写事件，调用selector.select阻塞等待写事件发生，发生后重新写。
        //如果没有提供Selector，CPU会空转。
        try {
            while ( (!timedout) && buf.hasRemaining() ) {
                int cnt = 0;
                if ( keycount > 0 ) { //only write if we were registered for a write
                    cnt = socket.write(buf); //write the data，直接写数据，返回写的字节数
                    if (cnt == -1) throw new EOFException();//

                    written += cnt;
                    if (cnt > 0) {//成功写一次重置timer计时器，继续写
                        time = System.currentTimeMillis(); //reset our timeout timer
                        continue; //we successfully wrote, try again without a selector
                    }
                    if (cnt==0 && (!block)) break; //don't block非阻塞写，并且什么内容都没写的话直接返回
                }
                if ( selector != null ) {
                    //register OP_WRITE to the selector 在selector上注册OP_WRITE事件，
                    if (key==null) key = socket.getIOChannel().register(selector, SelectionKey.OP_WRITE);
                    else key.interestOps(SelectionKey.OP_WRITE);
                    if (writeTimeout==0) {
                        timedout = buf.hasRemaining();
                    } else if (writeTimeout<0) {
                        keycount = selector.select();
                    } else {
                        keycount = selector.select(writeTimeout);
                    }
                }
                if (writeTimeout > 0 && (selector == null || keycount == 0) ) timedout = (System.currentTimeMillis()-time)>=writeTimeout;
            }//while
            if ( timedout ) throw new SocketTimeoutException();
        } finally {
            if (key != null) {
                key.cancel();
                if (selector != null) selector.selectNow();//removes the key from this selector
            }
        }
        return written;
    }

    /**
     * Performs a blocking read using the bytebuffer for data to be read and a selector to block.
     * If the <code>selector</code> parameter is null, then it will perform a busy read that could
     * take up a lot of CPU cycles.
     * @param buf ByteBuffer - the buffer containing the data, we will read as until we have read at least one byte or we timed out
     * @param socket SocketChannel - the socket to write data to
     * @param selector Selector - the selector to use for blocking, if null then a busy read will be initiated
     * @param readTimeout long - the timeout for this read operation in milliseconds, -1 means no timeout
     * @return int - returns the number of bytes read
     * @throws EOFException if read returns -1
     * @throws SocketTimeoutException if the read times out
     * @throws IOException if an IO Exception occurs in the underlying socket logic
     */
    public int read(ByteBuffer buf, NioChannel socket, Selector selector, long readTimeout) throws IOException {
        return read(buf,socket,selector,readTimeout,true);
    }
    /**
     * 读和上面写的逻辑一样。
     * Performs a read using the bytebuffer for data to be read and a selector to register for events should
     * you have the block=true.
     * If the <code>selector</code> parameter is null, then it will perform a busy read that could
     * take up a lot of CPU cycles.
     * @param buf ByteBuffer - the buffer containing the data, we will read as until we have read at least one byte or we timed out
     * @param socket SocketChannel - the socket to write data to
     * @param selector Selector - the selector to use for blocking, if null then a busy read will be initiated
     * @param readTimeout long - the timeout for this read operation in milliseconds, -1 means no timeout
     * @param block - true if you want to block until data becomes available or timeout time has been reached
     * @return int - returns the number of bytes read
     * @throws EOFException if read returns -1
     * @throws SocketTimeoutException if the read times out
     * @throws IOException if an IO Exception occurs in the underlying socket logic
     */
    public int read(ByteBuffer buf, NioChannel socket, Selector selector, long readTimeout, boolean block) throws IOException {
        if ( SHARED && block ) {
            return blockingSelector.read(buf,socket,readTimeout);
        }
        SelectionKey key = null;
        int read = 0;
        boolean timedout = false;
        int keycount = 1; //assume we can read
        long time = System.currentTimeMillis(); //start the timeout timer
        try {
            while ( (!timedout) ) {
                int cnt = 0;
                if ( keycount > 0 ) { //only read if we were registered for a read
                    cnt = socket.read(buf);
                    if (cnt == -1) {
                        if (read == 0) {
                            read = -1;
                        }
                        break;
                    }
                    read += cnt;
                    if (cnt > 0) continue; //read some more
                    if (cnt==0 && (read>0 || (!block) ) ) break; //we are done reading
                }
                if ( selector != null ) {//perform a blocking read
                    //register OP_WRITE to the selector
                    if (key==null) key = socket.getIOChannel().register(selector, SelectionKey.OP_READ);
                    else key.interestOps(SelectionKey.OP_READ);
                    if (readTimeout==0) {
                        timedout = (read==0);
                    } else if (readTimeout<0) {
                        keycount = selector.select();
                    } else {
                        keycount = selector.select(readTimeout);
                    }
                }
                if (readTimeout > 0 && (selector == null || keycount == 0) ) timedout = (System.currentTimeMillis()-time)>=readTimeout;
            }//while
            if ( timedout ) throw new SocketTimeoutException();
        } finally {
            if (key != null) {
                key.cancel();
                if (selector != null) selector.selectNow();//removes the key from this selector
            }
        }
        return read;
    }

    public void setMaxSelectors(int maxSelectors) {
        this.maxSelectors = maxSelectors;
    }

    public void setMaxSpareSelectors(int maxSpareSelectors) {
        this.maxSpareSelectors = maxSpareSelectors;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public void setSharedSelectorTimeout(long sharedSelectorTimeout) {
        this.sharedSelectorTimeout = sharedSelectorTimeout;
    }

    public int getMaxSelectors() {
        return maxSelectors;
    }

    public int getMaxSpareSelectors() {
        return maxSpareSelectors;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public long getSharedSelectorTimeout() {
        return sharedSelectorTimeout;
    }

    public ConcurrentLinkedQueue<Selector> getSelectors() {
        return selectors;
    }

    public AtomicInteger getSpare() {
        return spare;
    }
}