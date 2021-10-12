## flink sql 执行Jar包
   通过接收原生flink sql语句或者文本路径，执行flink sql任务。
    
###  通过set语法，传递CK配置。
```$xslt
set execution.checkpointing.interval = 3s;
create table user_info (
    userid bigint,
    username varchar,
    proctime as proctime()
) with (
    'connector' = 'kafka',
    'properties.bootstrap.servers' = 'localhost:9092',
    'topic' = 'mqtest02', "
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset'
);
create table user_sink (
    userid bigint,
    username varchar,
    primary key (userid) not enforced
) with (
    'connector' = 'print'
);
create view user_info_v as
  select userid, username  from user_info;
  
insert into user_sink
select
  u.userid,
  u.username 
from
  user_info  u;
```

### 本地udf调试，绑定udf包

```$xslt
sql 文本：

create  function str2low as 'cn.todd.flink.udf.strtolower' language scala;
create table source_table (
   id int,
   score int,
   address string
) with (
    'connector'='datagen',
    'rows-per-second'='2',
    'fields.id.kind'='sequence',
    'fields.id.start'='1',
    'fields.id.end'='100000',
    'fields.score.min'='1',
    'fields.score.max'='100',
    'fields.address.length'='10'
);

create table console_table (
     id int,
     score int,
     address string
) with (
    'connector' = 'print'
);

insert into console_table select id, score, str2low(address) from source_table;

properties 参数设置external.jars
Properties properties = new Properties();
properties.setProperty(
    StreamEnvConfigManager.EXTERNAL_JARS,
    "/Users/tal/code/flink-taste/flink-demo/target/flink-demo-1.0.jar");

new FlinkSqlExecutor().executeSqlText(testSql, properties);
```