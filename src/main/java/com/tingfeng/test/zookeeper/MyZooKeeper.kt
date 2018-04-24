package com.tingfeng.test.zookeeper

import org.apache.zookeeper.WatchedEvent
import org.apache.zookeeper.Watcher
import org.apache.zookeeper.ZooKeeper
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.concurrent.CountDownLatch

/**
 * 监听器
 */
class MyZooKeeper : Watcher {
    var logger = LoggerFactory.getLogger(MyZooKeeper::class.java)

    protected var countDownLatch = CountDownLatch(1)
    //缓存时间
    private val SESSION_TIME = 2000
    companion object {
        var zooKeeper: ZooKeeper? = null
    }


    /**
     * 监控所有被触发的事件
     */
    override fun process(event: WatchedEvent) {
        logger.info("收到事件通知：" + event.state)
        if (event.state == Watcher.Event.KeeperState.SyncConnected) {
            countDownLatch.countDown()
        }
    }

    /**
     *
     * 连接Zookeeper
     * <pre>
     * [关于connectString服务器地址配置]
     * 格式: 192.168.1.1:2181,192.168.1.2:2181,192.168.1.3:2181
     * 这个地址配置有多个ip:port之间逗号分隔,底层操作
     * ConnectStringParser connectStringParser =  new ConnectStringParser(“192.168.1.1:2181,192.168.1.2:2181,192.168.1.3:2181”);
     * 这个类主要就是解析传入地址列表字符串，将其它保存在一个ArrayList中
     * ArrayList<InetSocketAddress> serverAddresses = new ArrayList<InetSocketAddress>();
     * 接下去，这个地址列表会被进一步封装成StaticHostProvider对象，并且在运行过程中，一直是这个对象来维护整个地址列表。
     * ZK客户端将所有Server保存在一个List中，然后随机打乱(这个随机过程是一次性的)，并且形成一个环，具体使用的时候，从0号位开始一个一个使用。
     * 因此，Server地址能够重复配置，这样能够弥补客户端无法设置Server权重的缺陷，但是也会加大风险。
     * [客户端和服务端会话说明]
     * ZooKeeper中，客户端和服务端建立连接后，会话随之建立，生成一个全局唯一的会话ID(Session ID)。
     * 服务器和客户端之间维持的是一个长连接，在SESSION_TIMEOUT时间内，服务器会确定客户端是否正常连接(客户端会定时向服务器发送heart_beat，服务器重置下次SESSION_TIMEOUT时间)。
     * 因此，在正常情况下，Session一直有效，并且ZK集群所有机器上都保存这个Session信息。
     * 在出现网络或其它问题情况下（例如客户端所连接的那台ZK机器挂了，或是其它原因的网络闪断）,客户端与当前连接的那台服务器之间连接断了,
     * 这个时候客户端会主动在地址列表（实例化ZK对象的时候传入构造方法的那个参数connectString）中选择新的地址进行连接。
     * [会话时间]
     * 客户端并不是可以随意设置这个会话超时时间，在ZK服务器端对会话超时时间是有限制的，主要是minSessionTimeout和maxSessionTimeout这两个参数设置的。
     * 如果客户端设置的超时时间不在这个范围，那么会被强制设置为最大或最小时间。 默认的Session超时时间是在2 * tickTime ~ 20 * tickTime
    </InetSocketAddress></InetSocketAddress></pre> *
     * @param connectString  Zookeeper服务地址
     * @param sessionTimeout Zookeeper连接超时时间
     */
    fun connect(hosts: String) {
        try {
            if (zooKeeper == null) {
                // ZK客户端允许我们将ZK服务器的所有地址都配置在这里
                zooKeeper = ZooKeeper(hosts, SESSION_TIME, this)
                // 使用CountDownLatch.await()的线程（当前线程）阻塞直到所有其它拥有
                //CountDownLatch的线程执行完毕（countDown()结果为0）
                countDownLatch.await()
            }
        } catch (e: IOException) {
            logger.error("连接创建失败，发生 InterruptedException , e " + e.message, e)
        } catch (e: InterruptedException) {
            logger.error("连接创建失败，发生 IOException , e " + e.message, e)
        }

    }

    /**
     * 关闭连接
     */
    fun close() {
        try {
            if (zooKeeper != null) {
                zooKeeper!!.close()
            }
        } catch (e: InterruptedException) {
            logger.error("release connection error ," + e.message, e)
        }

    }
}