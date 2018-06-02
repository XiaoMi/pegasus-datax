# DataX Writer 插件文档


------------

## 1 快速介绍

Writer提供向[Pegasus](https://github.com/xiaomi/pegasus)系统的指定表中写入数据的功能。

## 2 功能与限制

* Pegasus是Key-Value系统，不支持Schema，所有类型的数据存储到Pegasus中时都需要转化为byte[]进行存储；
* 通过column配置列映射，指定name作为Pegasus存储的SortKey，指定index来确定哪一列作为Pegasus存储的Vale；
* 需要在column配置指定一个列作为Pegasus存储的HashKey，且这一列必须是unique的；

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

直接从Hive存储的HDFS向Pegasus系统导数据的配置样例（将aprefid列作为HashKey）：
```json
{
  "job": {
    "content": [
    {
      "reader": {
        "name": "hdfsreader",
        "parameter": {
          "defaultFS": "hdfs://xxx:port",
          "path": "/user/hive/warehouse/pegasus.db/test_table",
          "encoding": "UTF-8",
          "fileType": "orc",
          "column": ["*"]
        }
      },
      "writer": {
        "name": "pegasuswriter",
        "parameter": {
          "cluster": "xxx:31601,xxx:31601",
          "table": "datax_test",
          "encoding": "UTF-8",
          "timeout_ms": "10000",
          "ttl_seconds": "0",
          "retry_count": "2",
          "retry_delay_ms": "10000",
          "column": [
          {
            "name": "_hash_key_",
            "index": 0
          },
          {
            "name": "bssid",
            "index": 1
          },
          {
            "name": "ssid",
            "index": 2
          },
          {
            "name": "la_t",
            "index": 3
          },
          {
            "name": "lo_t",
            "index": 4
          },
          {
            "name": "label_t",
            "index": 5
          },
          {
            "name": "last_update_dt_t",
            "index": 6
          },
          {
            "name": "la_b",
            "index": 7
          },
          {
            "name": "lo_b",
            "index": 8
          },
          {
            "name": "label_b",
            "index": 9
          },
          {
            "name": "last_update_dt_b",
            "index": 10
          }
          ]
        }
      }
    }
    ],
    "setting": {
      "speed": {
        "channel": "100"
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

 	* 默认值：utf-8，**慎重修改** <br />

* **timeout_ms**

	* 描述：写数据的超时时间，单位毫秒。<br />

	* 必选：否 <br />

	* 默认值：10000，**建议在1000以上** <br />

* **ttl_seconds**

 	* 描述：写数据的TTL(Time-To-Live)时间，单位秒。 <br />

	* 必选：否 <br />

	* 默认值：0，表示数据不设置TTL限制 <br />

* **retry_count**

	* 描述：写数据失败后的重试次数。 <br />

	* 必选：否 <br />

	* 默认值：2 <br />

* **retry_delay_ms**

	* 描述：写数据失败后进行下一次重试的等待时间，单位毫秒。 <br />

	* 必选：否 <br />

	* 默认值：10000，**建议在1000以上** <br />

* **column**

		用户需要指定Column字段信息，配置如下：

		```json
		"column":
                 [
                            {
                                "name": "_hash_key_",
                                "index": 0
                            },
                            {
                                "name": "_empty_sort_key_",
                                "index": 1
                            },
                            {
                                "name": "some_column_name",
                                "index": 2
                            }
                 ]
		```

		其中name为"_hash_key_"和"_empty_sort_key_"有特殊意义：
		* "_hash_key_"指定的列作为Pegasus存储时的HashKey；
		* "_empty_sort_key_"指定的列在存储时使用空串("")作为SortKey；

		要求：
		* name不能有重复；
		* 必须指定"_hash_key_"列；
		* 除了"_hash_key_"列外，至少再指定一个数据列；

	* 必选：是 <br />

	* 默认值：无 <br />

### 3.3 类型转换

Pegasus不支持Schema，所有数据都以byte[]方式存储。<br />

目前所有类型的Column都先通过Column.asString()方法转化为String，然后通过String.getBytes()方法转化为byte[]。<br />

用户可以配置String.getBytes()方法的encoding类型。<br />

## 4 配置步骤

略

## 5 约束限制

略

## 6 FAQ

略
