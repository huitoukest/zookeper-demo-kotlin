package com.tingfeng.test.zookeeper




fun main(args:Array<String>){
    SingleMain.startTest()
}

object SingleMain {

    fun startTest() {
        // 定义父子类节点路径
        val rootPath = "/TestZookeeper"
        val child1Path = rootPath + "/hello1"
        val child2Path = rootPath + "/word1"

        //ZKOperate操作API
        val zkWatchAPI = ZKOperate()

        // 连接zk服务器
        val zooKeeper = MyZooKeeper()
        zooKeeper.connect("127.0.0.1:2181")

        // 创建节点数据
        if (zkWatchAPI.createZNode(rootPath, "<父>节点数据")) {
            println("节点[" + rootPath + "]数据内容[" + zkWatchAPI.readData(rootPath) + "]")
        }
        // 创建子节点, 读取 + 删除
        if (zkWatchAPI.createZNode(child1Path, "<父-子(1)>节点数据")) {
            println("节点[" + child1Path + "]数据内容[" + zkWatchAPI.readData(child1Path) + "]")
            zkWatchAPI.deteleZNode(child1Path)
            println("节点[" + child1Path + "]删除值后[" + zkWatchAPI.readData(child1Path) + "]")
        }

        // 创建子节点, 读取 + 修改
        if (zkWatchAPI.createZNode(child2Path, "<父-子(2)>节点数据")) {
            println("节点[" + child2Path + "]数据内容[" + zkWatchAPI.readData(child2Path) + "]")
            zkWatchAPI.updateZNodeData(child2Path, "<父-子(2)>节点数据,更新后的数据")
            println("节点[" + child2Path + "]数据内容更新后[" + zkWatchAPI.readData(child2Path) + "]")
        }

        // 获取子节点
        val childPaths = zkWatchAPI.getChild(rootPath)
        if (null != childPaths) {
            println("节点[" + rootPath + "]下的子节点数[" + childPaths.size + "]")
            for (childPath in childPaths) {
                println(" |--节点名[$childPath]")
            }
        }
        // 判断节点是否存在
        println("检测节点[" + rootPath + "]是否存在:" + zkWatchAPI.isExists(rootPath))
        println("检测节点[" + child1Path + "]是否存在:" + zkWatchAPI.isExists(child1Path))
        println("检测节点[" + child2Path + "]是否存在:" + zkWatchAPI.isExists(child2Path))


        zooKeeper.close()
    }
}

