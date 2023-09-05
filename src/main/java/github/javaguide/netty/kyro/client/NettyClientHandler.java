package github.javaguide.netty.kyro.client;

import github.javaguide.netty.kyro.dto.RpcResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author shuang.kou
 * @createTime 2020年05月13日 20:50:00
 */

/**
 * 读取服务端发送过来的 RpcResponse 消息对象，并将 RpcResponse 消息对象保存到 AttributeMap 上
 */
public class NettyClientHandler extends ChannelInboundHandlerAdapter {
    private static final Logger logger = LoggerFactory.getLogger(NettyClientHandler.class);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        try {
            RpcResponse rpcResponse = (RpcResponse) msg;
            logger.info("client receive msg: [{}]", rpcResponse.toString());
            // 声明一个 AttributeKey 对象
            AttributeKey<RpcResponse> key = AttributeKey.valueOf("rpcResponse");
            /**
             * ctx.channel()获取当前的Channel，Channel 实现了 AttributeMap 接口
             * AttributeMap 可以看作是一个Channel的共享数据源，类Map数据结构，key是AttributeKey，value是Attribute
             * 将服务端返回的结果保存到通道的属性中（AttributeMap），使用 set() 方法设置属性值。这里的 ctx 表示 ChannelHandlerContext，代表当前的处理器上下文
             */
            ctx.channel().attr(key).set(rpcResponse);
            ctx.channel().close();
        } finally {
            // 释放消息对象的引用计数
            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("client caught exception", cause);
        ctx.close();
    }
}

