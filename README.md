.net 开发kafka，这今天绕了不少坑，好多人不负责任瞎写，网上dll都没有一个，自己编译了一个，结果发现一个好网站：nuget 查看开源地址和最新release包https://www.nuget.org ，现在比较好的就两个kafka-net和rdkafka。经过实际测试，还是rdfakfa更胜一筹，因为他是基于librdkafka做二次开发的。下面给出github链接地址:
 - kafka-net:  https://github.com/Jroland/kafka-net
 - rdkafka:    https://github.com/ah-/rdkafka-dotnet

dll包放到dll文件夹里面，地址在：https://github.com/bluetom520/kafka_for_net，下面分别用两种dll举例


### 1 kafka-net
kafka-net的消费者不能指定latest，只能从开头开始消费，没找到解决办法，无奈放弃，生产者还是不错的，消费有延迟
代码基于.net 4.0
### 2 rdkafka
生产者和消费者效能都很高，实时，可进行config配置参数，达到各种效果，具体参考https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
代码基于 .net 4.5.1
rdkafka基于librdkafka：https://github.com/edenhill/librdkafka，需要在debug or release导入两个dll 
```
zlib.dll
librdkafka.dll
```
### 3 具体参考我的博客
https://note.gitcloud.cc/blog/bluetom520
