package com.gk.rpc;

import com.gk.rpc.constants.ConfigurationKeys;
import com.gk.rpc.netty.NettyRemotingServer;
import com.gk.rpc.netty.NettyServerConfig;
import com.gk.rpc.session.SessionHolder;
import com.gk.rpc.utils.PortHelper;
import io.seata.XID;
import io.seata.thread.NamedThreadFactory;
import io.seata.utils.NetUtil;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        int port = PortHelper.getPort(args);
        System.setProperty(ConfigurationKeys.SERVER_PORT, Integer.toString(port));


        ThreadPoolExecutor workingThreads = new ThreadPoolExecutor(NettyServerConfig.getMinServerPoolSize(),
                NettyServerConfig.getMaxServerPoolSize(), NettyServerConfig.getKeepAliveTime(), TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(NettyServerConfig.getMaxTaskQueueSize()),
                new NamedThreadFactory("ServerHandlerThread", NettyServerConfig.getMaxServerPoolSize()), new ThreadPoolExecutor.CallerRunsPolicy());

        ParameterParser parameterParser = new ParameterParser(args);

        NettyRemotingServer nettyRemotingServer = new NettyRemotingServer(workingThreads);
        //server port
        nettyRemotingServer.setListenPort(parameterParser.getPort());
        UUIDGenerator.init(parameterParser.getServerNode());
        //序列化session
        SessionHolder.init(parameterParser.getStoreMode());

        if (NetUtil.isValidIp(parameterParser.getHost(), false)) {
            XID.setIpAddress(parameterParser.getHost());
        } else {
            XID.setIpAddress(NetUtil.getLocalIp());
        }
        XID.setPort(nettyRemotingServer.getListenPort());
        try {
            nettyRemotingServer.init();
        } catch (Throwable e) {
            System.exit(-1);
        }

        System.exit(0);
    }
}
