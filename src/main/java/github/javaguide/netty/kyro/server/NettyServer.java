package github.javaguide.netty.kyro.server;

import github.javaguide.netty.kyro.codec.NettyKryoDecoder;
import github.javaguide.netty.kyro.codec.NettyKryoEncoder;
import github.javaguide.netty.kyro.dto.RpcRequest;
import github.javaguide.netty.kyro.dto.RpcResponse;
import github.javaguide.netty.kyro.serialize.KryoSerializer;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author shuang.kou
 * @createTime 2020年05月13日 19:19:00
 */
public class NettyServer {
    private static final Logger logger = LoggerFactory.getLogger(NettyServer.class);
    private final int port;

    private NettyServer(int port) {
        this.port = port;
    }

    private void run() {
        // 用于处理客户端的 TCP 连接请求
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        // 负责每一条连接的具体读写数据的处理逻辑，真正负责 I/O 读写操作，交由对应的 Handler 处理
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        KryoSerializer kryoSerializer = new KryoSerializer();
        try {
            ServerBootstrap b = new ServerBootstrap();
            // 配置和启动服务器端
            b.group(bossGroup, workerGroup) // 给引导类配置两大线程组,确定了线程模型
                    .channel(NioServerSocketChannel.class)
                    // TCP默认开启了 Nagle 算法，该算法的作用是尽可能的发送大数据块，减少网络传输。TCP_NODELAY 参数的作用就是控制是否启用 Nagle 算法。
                    .childOption(ChannelOption.TCP_NODELAY, true) // 禁止Nagle算法可以避免网络数据在传递过程中的延迟
                    // 是否开启 TCP 底层心跳机制
                    .childOption(ChannelOption.SO_KEEPALIVE, true) // 开启 TCP 底层的心跳机制，保持长连接
                    // 表示系统用于临时存放已完成三次握手的请求的队列的最大长度,如果连接建立频繁
                    // 服务器处理创建新连接较慢，可以适当调大这个参数
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new NettyKryoDecoder(kryoSerializer, RpcRequest.class));
                            ch.pipeline().addLast(new NettyKryoEncoder(kryoSerializer, RpcResponse.class));
                            ch.pipeline().addLast(new NettyServerHandler());
                        }
                    });

            // 绑定端口，同步等待绑定成功
            ChannelFuture f = b.bind(port).sync();
            /**
             * 阻塞等待服务端监听端口关闭：当服务端决定关闭时，会调用close()方法关闭通道，进而触发通道关闭事件，这时主线程的sync()方法就会返回，继续执行后续逻辑，最终关闭服务端
             */
            f.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            logger.error("occur exception when start server:", e);
        } finally {
            // 优雅关闭相关线程组资源
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    public static void main(String[] args) {
        new NettyServer(8889).run();
    }
}
