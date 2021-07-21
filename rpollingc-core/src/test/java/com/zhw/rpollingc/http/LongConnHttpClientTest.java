package com.zhw.rpollingc.http;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.zhw.rpollingc.common.TypeReference;
import com.zhw.rpollingc.http.protocol.HttpJsonCodec;
import com.zhw.rpollingc.http.protocol.HttpPlainCodec;
import com.zhw.rpollingc.http.protocol.ReqOptions;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class LongConnHttpClientTest {


    @org.junit.Test
    public void close() {
    }

    @org.junit.Test
    public void get() {

        NettyConfig config = new NettyConfig("localhost", 8801);
//        NettyConfig config = new NettyConfig("api2.wlc.nppa.gov.cn", 80);
        config.setConnNum(1);
//        config.setRemoteHost("localhost");
        config.setMaxWaitingReSendReq(5000);

//        HttpJsonCodec codec = new HttpJsonCodec(new PooledByteBufAllocator(true),
//                new ObjectMapper(), false, "localhost");
        HttpJsonCodec codec = new HttpJsonCodec(new UnpooledByteBufAllocator(false),
                new ObjectMapper(), false, "localhost");
        LongConnHttpClient client = new LongConnHttpClient(config, codec);
        client.connect();

        ReqOptions options = new ReqOptions(TypeReference.from(Person.class));
//        HttpResponse httpResponse = client.post("/behavior/collection/loginout", "1234", options);
//        print(httpResponse);

        int num = 10;
        CountDownLatch latch = new CountDownLatch(num);
        AtomicInteger errs = new AtomicInteger(0);
        AtomicInteger success = new AtomicInteger(0);
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < num; i++) {
            client.postAsync("/zjson", "12998", options, response -> {
                print(response);
                success.incrementAndGet();
                latch.countDown();
            }, e -> {
                e.printStackTrace();
                errs.incrementAndGet();
                latch.countDown();
            });

        }
        System.out.println("-------------" + (System.currentTimeMillis() - startTime));
        try {
            latch.await();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("success:" + success.get() + " ,errs:" + errs.get() + "-------------" + (System.currentTimeMillis() - startTime));
        try {
            synchronized (this) {
                this.wait(10000);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void print(HttpResponse response) {
        if (null != response.content()) {
            System.out.println(response.content());
        } else {
            System.out.println(response.err());
        }
    }

    @org.junit.Test
    public void post() {


        PersonBuilder personBuilder = new PersonBuilder();
    }

    @org.junit.Test
    public void getAsync() {
    }

    @org.junit.Test
    public void postAsync() {
    }
}
