package com.zhw.rpollingc.http.conn;

import com.zhw.rpollingc.common.RpcException;
import com.zhw.rpollingc.http.NettyConfig;
import io.netty.util.internal.SystemPropertyUtil;
import io.netty.util.internal.ThreadLocalRandom;

public class MultiConnHttpEndPoint implements HttpEndPoint {

    private final HttpEndPoint[] endPoints;
    private final int connNum;

    public MultiConnHttpEndPoint(NettyConfig config) {
        int vm_num = SystemPropertyUtil.getInt("rpollingc.connection.num", 0);
        if (vm_num > 0) {
            this.connNum = vm_num;
        } else {
            this.connNum = config.getConnNum();
        }
//        if (this.connNum < 2) {
//            throw new IllegalArgumentException("connNum < 2");
//        }
        HttpConnectionFactory factory = new HttpConnectionFactory(config);

        HttpConnection first = factory.createConnection();
        HttpConnection t = first;

        endPoints = new HttpEndPoint[connNum];
        endPoints[0] = t;
        for (int i = 1; i < connNum; i++) {
            endPoints[i] = t.next = factory.createConnection();
            t = t.next;
        }
        t.next = first;
    }

    @Override
    public void send(HttpRequest request) throws RpcException {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int i = random.nextInt(connNum);
        endPoints[i].send(request);
    }

    @Override
    public void connect() {
        for (int i = 0; i < connNum; i++) {
            endPoints[i].connect();
        }
    }

    @Override
    public void close() {
        for (int i = 0; i < connNum; i++) {
            endPoints[i].close();
        }
    }
}
