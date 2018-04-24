package com.tingfeng.test.zookeeper


import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.ZooDefs
import org.slf4j.LoggerFactory

class ZKOperate {

    internal var logger = LoggerFactory.getLogger(MyZooKeeper::class.java)

    internal var myZooKeeper = MyZooKeeper()

    /**
     *
     * 创建zNode节点, String create(path<节点路径>, data[]<节点内容>, List(ACL访问控制列表), CreateMode<zNode创建类型>) </zNode创建类型></节点内容></节点路径><br></br>
     * <pre>
     * 节点创建类型(CreateMode)
     * 1、PERSISTENT:持久化节点
     * 2、PERSISTENT_SEQUENTIAL:顺序自动编号持久化节点，这种节点会根据当前已存在的节点数自动加 1
     * 3、EPHEMERAL:临时节点客户端,session超时这类节点就会被自动删除
     * 4、EPHEMERAL_SEQUENTIAL:临时自动编号节点
    </pre> *
     * @param path zNode节点路径
     * @param data zNode数据内容
     * @return 创建成功返回true, 反之返回false.
     */
    fun createZNode(path: String, data: String): Boolean {
        try {
            val zkPath = MyZooKeeper.zooKeeper!!.create(path, data.toByteArray(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
            logger.info("ZooKeeper创建节点成功，节点地址：" + zkPath)
            return true
        } catch (e: KeeperException) {
            logger.error("创建节点失败：" + e.message + "，path:" + path, e)
        } catch (e: InterruptedException) {
            logger.error("创建节点失败：" + e.message + "，path:" + path, e)
        }

        return false
    }

    /**
     *
     * 删除一个zMode节点, void delete(path<节点路径>, stat<数据版本号>)</数据版本号></节点路径><br></br>
     * <pre>
     * 说明
     * 1、版本号不一致,无法进行数据删除操作.
     * 2、如果版本号与znode的版本号不一致,将无法删除,是一种乐观加锁机制;如果将版本号设置为-1,不会去检测版本,直接删除.
    </pre> *
     * @param path zNode节点路径
     * @return 删除成功返回true,反之返回false.
     */
    fun deteleZNode(path: String): Boolean {
        try {
            MyZooKeeper.zooKeeper!!.delete(path, -1)
            logger.info("ZooKeeper删除节点成功，节点地址：" + path)
            return true
        } catch (e: InterruptedException) {
            logger.error("删除节点失败：" + e.message + "，path:" + path, e)
        } catch (e: KeeperException) {
            logger.error("删除节点失败：" + e.message + "，path:" + path, e)
        }

        return false
    }

    /**
     *
     * 更新指定节点数据内容, Stat setData(path<节点路径>, data[]<节点内容>, stat<数据版本号>)</数据版本号></节点内容></节点路径>
     * <pre>
     * 设置某个znode上的数据时如果为-1，跳过版本检查
    </pre> *
     * @param path zNode节点路径
     * @param data zNode数据内容
     * @return 更新成功返回true,返回返回false
     */
    fun updateZNodeData(path: String, data: String): Boolean {
        try {
            val stat = MyZooKeeper.zooKeeper!!.setData(path, data.toByteArray(), -1)
            logger.info("更新数据成功, path：$path, stat: $stat")
            return true
        } catch (e: KeeperException) {
            logger.error("更新节点数据失败：" + e.message + "，path:" + path, e)
        } catch (e: InterruptedException) {
            logger.error("更新节点数据失败：" + e.message + "，path:" + path, e)
        }

        return false
    }

    /**
     *
     * 读取指定节点数据内容,byte[] getData(path<节点路径>, watcher<监视器>, stat<数据版本号>)</数据版本号></监视器></节点路径>
     * @param path zNode节点路径
     * @return 节点存储的值,有值返回,无值返回null
     */
    fun readData(path: String): String? {
        var data: String? = null
        try {
            data = String(MyZooKeeper.zooKeeper!!.getData(path, false, null))
            logger.info("读取数据成功, path:$path, content:$data")
        } catch (e: KeeperException) {
            logger.error("读取数据失败,发生KeeperException! path: " + path
                    + ", errMsg:" + e.message, e)
        } catch (e: InterruptedException) {
            logger.error("读取数据失败,发生InterruptedException! path: " + path
                    + ", errMsg:" + e.message, e)
        }

        return data
    }

    /**
     *
     * 获取某个节点下的所有子节点,List getChildren(path<节点路径>, watcher<监视器>)该方法有多个重载</监视器></节点路径>
     * @param path zNode节点路径
     * @return 子节点路径集合 说明,这里返回的值为节点名
     * <pre>
     * eg.
     * /node
     * /node/child1
     * /node/child2
     * getChild( "node" )户的集合中的值为["child1","child2"]
    </pre> *
     * @throws KeeperException
     * @throws InterruptedException
     */
    fun getChild(path: String): List<String>? {
        try {
            val list = MyZooKeeper.zooKeeper!!.getChildren(path, false)
            if (list.isEmpty()) {
                logger.info("中没有节点" + path)
            }
            return list
        } catch (e: KeeperException) {
            logger.error("读取子节点数据失败,发生KeeperException! path: " + path
                    + ", errMsg:" + e.message, e)
        } catch (e: InterruptedException) {
            logger.error("读取子节点数据失败,发生InterruptedException! path: " + path
                    + ", errMsg:" + e.message, e)
        }

        return null
    }

    /**
     *
     * 判断某个zNode节点是否存在, Stat exists(path<节点路径>, watch<并设置是否监控这个目录节点></并设置是否监控这个目录节点>，这里的 watcher 是在创建 ZooKeeper 实例时指定的 watcher>)</节点路径>
     * @param path zNode节点路径
     * @return 存在返回true,反之返回false
     */
    fun isExists(path: String): Boolean {
        try {
            val stat = MyZooKeeper.zooKeeper!!.exists(path, false)
            return null != stat
        } catch (e: KeeperException) {
            logger.error("读取数据失败,发生KeeperException! path: " + path
                    + ", errMsg:" + e.message, e)
        } catch (e: InterruptedException) {
            logger.error("读取数据失败,发生InterruptedException! path: " + path
                    + ", errMsg:" + e.message, e)
        }

        return false
    }
}