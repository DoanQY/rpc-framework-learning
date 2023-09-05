package github.javaguide.netty.kyro.server;

import github.javaguide.netty.kyro.dto.RpcRequest;
import github.javaguide.netty.kyro.dto.RpcResponse;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author shuang.kou
 * @createTime 2020年05月13日 20:44:00
 */

/**
 * 用于处理入站的事件和操作
 */
public class NettyServerHandler extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(NettyServerHandler.class);
    private static final AtomicInteger atomicInteger = new AtomicInteger(1);

    /**
     * 当从通道（channel）读取到数据时被调用
     * @param ctx 通道上下文（业务数据）
     * @param msg 读取到的数据
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        try {
            RpcRequest rpcRequest = (RpcRequest) msg;
            logger.info("server receive msg: [{}] ,times:[{}]", rpcRequest, atomicInteger.getAndIncrement());
            RpcResponse messageFromServer = RpcResponse.builder().message("message from server").build();
            // 写入并刷新到远程
            ChannelFuture f = ctx.writeAndFlush(messageFromServer);
            /**
             * 为发送操作添加一个监听器，当发送操作完成时，会关闭通道
             * ChannelFutureListener.CLOSE 是一个预定义的监听器，它会在操作完成时自动触发关闭
             * 通道的动作
             */
            f.addListener(ChannelFutureListener.CLOSE);
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("server catch exception", cause);
        ctx.close();
    }
}
