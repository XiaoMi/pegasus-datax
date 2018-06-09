# DataX Writer 插件文档


------------

## 1 快速介绍

该Writer提供向[Pegasus](https://github.com/xiaomi/pegasus)系统的指定表中写入数据的功能。

## 2 功能与限制

* 写数据使用[Pegasus Java Client](https://github.com/xiaomi/pegasus-java-client)，当前使用[1.8.0-thrift-0.11.0-inlined-release](https://github.com/XiaoMi/pegasus-java-client/releases/tag/1.8.0-thrift-0.11.0-inlined-release)版本，你需要先maven install该客户端库；
* Pegasus是Key-Value系统，不支持Schema，所有类型的数据存储到Pegasus中时都需要转化为byte[]进行存储；
* 通过column配置列映射，name作为Pegasus存储的SortKey，index确定哪一列作为Value；
* 需要在column配置中指定一个列作为Pegasus存储的HashKey，且这一列必须是unique的；

## 3 功能说明

### 3.1 配置样例

假设数据源来自Hive，表结构为：
```
CREATE TABLE test_table (
aprefid STRING,
bssid STRING,
ssid STRING,
la_t STRING,
lo_t STRING,
label_t STRING,
last_update_dt_t STRING,
la_b STRING,
lo_b STRING,
label_b STRING,
last_update_dt_b STRING
)
COMMENT 'This is a test table'
stored AS ORC;
```

配置样例```hdfs2pegasus.json```，从Hive所存储的HDFS向Pegasus系统导数据，并将aprefid列作为HashKey：
```json
{
    "job":{
        "content":[
            {
                "reader":{
                    "name":"hdfsreader",
                    "parameter":{
                        "defaultFS":"hdfs://xxx:port",
                        "path":"/user/hive/warehouse/pegasus.db/test_table",
                        "encoding":"UTF-8",
                        "fileType":"orc",
                        "column":[
                            "*"
                        ]
                    }
                },
                "writer":{
                    "name":"pegasuswriter",
                    "parameter":{
                        "cluster":"x.x.x.x:34601,x.x.x.x:34601",
                        "table":"datax_test",
                        "encoding":"UTF-8",
                        "timeout_ms":"10000",
                        "ttl_seconds":"0",
                        "retry_count":"2",
                        "retry_delay_ms":"10000",
                        "mapping":{
                            "hash_key":"${0}",
                            "values":[
                                {
                                    "sort_key":"",
                                    "value":"${1}"
                                },
                                {
                                    "sort_key":"la_t",
                                    "value":"${3}"
                                },
                                {
                                    "sort_key":"lo_t",
                                    "value":"${4}"
                                },
                                {
                                    "sort_key":"${6}",
                                    "value":"${6},${10}"
                                }
                            ]
                        }
                    }
                }
            }
        ],
        "setting":{
            "speed":{
                "channel":"1"
            }
        }
    }
}
```

### 3.2 参数说明

* **cluster**

	* 描述：Pegasus集群的meta-server地址列表。格式：host1:port1,host2:port2 。<br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **table**

	* 描述：要写入的表名。 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **encoding**

	* 描述：写数据时将String转化为byte[]的编码配置。<br />

 	* 必选：否 <br />

 	* 默认值：UTF-8，**慎重修改** <br />

* **timeout_ms**

	* 描述：写数据操作的超时时间，单位毫秒。<br />

	* 必选：否 <br />

	* 默认值：10000，**建议在1000以上** <br />

* **ttl_seconds**

 	* 描述：写数据的TTL(Time-To-Live)时间限制，单位秒。 <br />

	* 必选：否 <br />

	* 默认值：0，表示数据不设置TTL限制 <br />

* **retry_count**

	* 描述：写数据失败后的重试次数。 <br />

	* 必选：否 <br />

	* 默认值：2 <br />

* **retry_delay_ms**

	* 描述：写数据失败后等待下一次重试的时间，单位毫秒。 <br />

	* 必选：否 <br />

	* 默认值：10000，**建议在1000以上** <br />

* **mapping**

	用户需要指定Column字段到Pegasus存储的映射关系，配置如下：

	```json
    "mapping":{
        "hash_key":"",
        "values":[
            {
                "sort_key":"",
                "value":""
            },
            {
                "sort_key":"",
                "value":""
            }
        ]
    }
	```

	* 说明：
	  * mapping中必须指定hash_key和values；
	  * hash_key的值不能为空；
	  * values的列表不能为空，且列表中每个元素必须指定sort_key和value，不允许出现重复的sort_key；
	  * sort_key和value的值可以为空；
	  * hash_key、sort_key和value的值中可以通过${i}的方式嵌入Column[i]的值：
	    * ${i}可以出现多次，所有地方都会被替换；
	    * 如果i不是整数或者Column[i]不存在，则保留原状，不进行替换；
	    * 如果想表达原始的"$"符号，可以用"$$"进转义，譬如"$${1}"就不会被Column[1]替换，而是转换为"${1}"；

	* 必选：是 <br />

	* 默认值：无 <br />

### 3.3 类型转换

Pegasus不支持Schema，所有数据都以byte[]方式存储。<br />

目前所有类型的Column都先通过Column.asString()方法转化为String，然后通过String.getBytes()方法转化为byte[]。<br />

用户可以配置String.getBytes()方法的encoding类型。<br />

## 4 配置步骤

* 下载和安装Pegasus Java Client

```bash
git clone https://github.com/xiaomi/pegasus-java-client
cd pegasus-java-client
git checkout 1.8.0-thrift-0.11.0-inlined-release
mvn clean install -DskipTests
```

* 下载和编译DataX：

```bash
git clone https://github.com/xiaomi/pegasus-datax.git
cd pegasus-datax
mvn -U clean package assembly:assembly -Dmaven.test.skip=true
```

* 参照上面的配置样例，准备配置文件```hdfs2pegasus.json```。

* 运行：

```base
python target/datax/datax/bin/datax.py ./hdfs2pegasus.json
```

## 5 约束限制

略

## 6 FAQ

略

