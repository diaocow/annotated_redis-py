annotated-redis-py
==================

Redis python客户端：redis-py 源码注释



#### 关于

本项目是Redis Python客户端redis-py_2.8.0源码注释版，原始代码： https://github.com/andymccurdy/redis-py  
  
redis-py代码本身非常精简（主要集中在client.py和connection.py这两个文件），该项目对redis-py中常用类譬如StrictRedis，PubSub，Pipeline等能做了详细注释，并且包含相应的使用方法

#### 未完成部分：
redis sentinel模块注释

#### 备注
 * StrictRedis类中有很多命令方法，譬如：set，get，lpush，bpop等，本来想为这些方法做注释，但发觉详说的话占太多篇幅（譬如bpop这个命令就能扯很多），但浅尝辄止的化又意义不大（满屏无意义的内容），所以把命令这块还是留给读者自己，可以参考redis的官方文档：

 * 了解Redis Command协议对理解代码有很大帮助，可以参看：
  
  

**diaocow**

