package com.zhw.rpollingc.http;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.zhw.rpollingc.common.RpcException;
import com.zhw.rpollingc.http.conn.HttpEndPoint;
import com.zhw.rpollingc.http.conn.HttpRequest;
import com.zhw.rpollingc.http.conn.MultiConnHttpEndPoint;
import com.zhw.rpollingc.http.protocol.HttpCodec;
import com.zhw.rpollingc.http.protocol.ReqOptions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.util.ReferenceCounted;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class LongConnHttpClient implements HttpClient<ReqOptions> {

    public static final long timeoutMs = 10000L;

    private final HttpEndPoint endPoint;

    @Nullable
    private final HttpCodec codec;

    private final ExecutorService executorService;

    // private final ThreadFactory defaultThreadFactory = new DefaultThreadFactory("longConnHttpClient-", false);
    private final ThreadFactory defaultThreadFactory = new ThreadFactoryBuilder().setNameFormat("longConnHttpClient-%d").build();

    public LongConnHttpClient(@NonNull NettyConfig config, @Nullable HttpCodec codec) {
        if (config == null) {
            throw new NullPointerException("config");
        }
        this.codec = codec;

        endPoint = new MultiConnHttpEndPoint(config);
        executorService = new ThreadPoolExecutor(0, 16 /*Math.min(Runtime.getRuntime().availableProcessors(), 16)*/,
                60L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(50), defaultThreadFactory);
    }

    private static void release(ReferenceCounted ref) {
        int refCnt;
        if (ref != null && (refCnt = ref.refCnt()) != 0) {
            ref.release(refCnt);
        }
    }

    public void connect() {
        endPoint.connect();
    }

    public void close() {
        executorService.shutdown();
        endPoint.close();
    }

    /**
     * 包装的请求对象
     */
    private class Req extends HttpRequest<ReqOptions> {
        private ByteBuf byteBuf;
        private final BiConsumer<FullHttpResponse, Req> onResp;
        private final BiConsumer<Throwable, Req> onErr;
        private final ReqOptions options;

        public Req(HttpMethod method, String service, Object body,
                   BiConsumer<FullHttpResponse, Req> onResp,
                   BiConsumer<Throwable, Req> onErr,
                   ReqOptions options) {
            super(method, service, body);
            this.onResp = onResp;
            this.onErr = onErr;
            this.options = options;
        }

        @Override
        protected ByteBuf getReqByteBuf() {
            return byteBuf;
        }

        public void setByteBuf(ByteBuf byteBuf) {
            this.byteBuf = byteBuf;
        }


        @Override
        public ReqOptions getOptions() {
            return options;
        }

        @Override
        public void onErr(Throwable err) {
            release(byteBuf);
            onErr.accept(err, this);
        }


        @Override
        public void onResp(FullHttpResponse response) {
            release(byteBuf);
            onResp.accept(response, this);
        }
    }

    /**
     * 包装的响应对象
     */
    private static class Res<T> {
        T res;
        Throwable err;
    }


    private Req encodeReq(HttpMethod method, String url, Object body,
                          BiConsumer<FullHttpResponse, Req> onResp,
                          BiConsumer<Throwable, Req> onErr,
                          ReqOptions options) throws RpcException {

        Req request = new Req(method, url, body, onResp, onErr, options);
        ByteBuf byteBuf;
        if (null != codec) {
            byteBuf = codec.encode(request);
        } else {
            byteBuf = Unpooled.buffer();
        }
        request.setByteBuf(byteBuf);
        return request;
    }

    @Override
    public HttpResponse get(String url, ReqOptions options) throws RpcException {
        return sync(HttpMethod.GET, url, null, options);
    }

    @Override
    public HttpResponse post(String url, Object body, ReqOptions options) throws RpcException {
        return sync(HttpMethod.POST, url, body, options);
    }

    private HttpResponse sync(HttpMethod method, String url, Object body, ReqOptions options) {
        Res<FullHttpResponse> res = new Res<>();

        CountDownLatch latch = new CountDownLatch(1);
        BiConsumer<Throwable, Req> onErr = (e, r) -> {
            res.err = e;
            latch.countDown();
        };

        BiConsumer<FullHttpResponse, Req> resp = (r, req) -> {
            res.res = r;
            latch.countDown();
        };

        Req req = encodeReq(method, url, body, resp, onErr, options);
        endPoint.send(req);

        try {
            latch.await(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RpcException(e);
        }
        if (res.err != null) {
            if (res.err instanceof RpcException) {
                throw (RpcException) res.err;
            } else {
                throw new RpcException(res.err.getMessage(), res.err);
            }
        }
        if (res.res == null) {
            throw new RpcException("timeout");
        }
        try {
            return codec.decode(res.res, req);
        } catch (RpcException e) {
            throw e;
        } catch (Throwable e) {
            throw new RpcException(e);
        } finally {
            release(res.res);
        }
    }


    @Override
    public void getAsync(String url,
                         ReqOptions options,
                         Consumer<HttpResponse> onResp,
                         Consumer<Throwable> onErr) {
        async(HttpMethod.GET, url, null, onResp, onErr, options);
    }

    @Override
    public void postAsync(String url,
                          Object body,
                          ReqOptions options,
                          Consumer<HttpResponse> onResp,
                          Consumer<Throwable> onErr) {
        async(HttpMethod.POST, url, body, onResp, onErr, options);
    }

    private void async(HttpMethod method, String url, Object body,
                       Consumer<HttpResponse> onResp,
                       Consumer<Throwable> onErr, ReqOptions options) {

        BiConsumer<FullHttpResponse, Req> resp = (frs, r) -> {
            try {
                executorService.execute(() -> {
                    HttpResponse response;
                    try {
                        response = codec.decode(frs, r);
                    } catch (Throwable e) {
                        onErr.accept(e);
                        return;
                    } finally {
                        release(frs);
                    }
                    onResp.accept(response);

                });
            } catch (Throwable e) {
                release(frs);
                onErr.accept(e);
            }
        };
        BiConsumer<Throwable, Req> err = (e, r) -> onErr.accept(e);

        try {
            Req req = encodeReq(method, url, body, resp, err, options);
            endPoint.send(req);
        } catch (Throwable e) {
            onErr.accept(e);
        }
    }


}
