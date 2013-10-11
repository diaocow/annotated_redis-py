annotated-redis-py
==================

Redis python客户端：redis-py 源码注释



### 关于

本项目是Redis Python客户端redis-py_2.8.0源码注释版，原始代码： https://github.com/andymccurdy/redis-py  
  
redis-py代码本身非常精简（主要集中在client.py和connection.py这两个文件），该项目对redis-py中常用类，譬如：StrictRedis，PubSub，Pipeline等能做了详细注释，并且包含相应的使用方法

### 未完成部分：
 * Redis sentinel 模块注释  
 
 * Lua script 模块注释

### 备注
 * StrictRedis类中有很多命令方法，譬如：set，get，lpush，bpop等，本来想为这些方法做注释，但发觉详说太占篇幅，浅尝辄止的话又意义不大，所以把命令这块还是留给读者自己，可以参考redis的官方文档：http://redis.io/commands
 
 * 了解redis请求协议对理解代码有很大帮助（读者自己可以通过telent或者nc命令，查看redis原始数据流），reids请求协议可以参看官方文档：http://redis.io/topics/protocol
 
 * 本人redis学习相关文章：http://diaocow.iteye.com/category/291663
  
  
