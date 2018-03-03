# Frog

本项目为示例程序，还有很多需要完善的地方。

## 使用

转换、清洗开发主要分为 3 部分：

1. 定义数据源，即后续操作的基础数据源；
2. 定义清洗逻辑，即基于基础数据的处理，得到我们需要的结果；
3. 定义数据持久化，即我们最终的结果数据存储持久化。

## 转换清洗开发示例 - transform.xml

```xml
<?xml version="1.0" encoding="UTF-8"?>
<logic>
    <!-- 替换参数定义，可用于后面开发进行简单参数化定义 -->
    <parameters>
        <parameter name="path" value="/Users/mars/Desktop/Data/tools" />
        <parameter name="path1" value="file:///Users/mars/Desktop/Data/bank/bank/people" />
        <parameter name="path2" value="file:///Users/mars/Desktop/Data/bank/bank/people_append" />
        <parameter name="path3" value="file:///Users/mars/Desktop/Data/bank/bank/people_out1" />
        <parameter name="path4" value="file:///Users/mars/Desktop/Data/bank/bank/people_out2" />
        <parameter name="path5" value="file:///Users/mars/Desktop/Data/bank/bank/people_out3" />
    </parameters>

    <base>
        <!-- ${path}: 上述paramters定义的替换参数 -->
        <logPath>${path}/logs</logPath>
        <metaPath>${path}/metapath</metaPath>
        <tempPath>${path}/tmp</tempPath>
        <schemaPath>${path}/schemapath</schemaPath>
        <database>test</database>
    </base>

    <!-- 用于后续转换、清洗等操作的基础数据 -->
    <sources>
        <source tableName="people1" type="parquet" partitions="1" cache="true" dataPath="${path1}"/>
        <source tableName="people2" type="parquet" partitions="1" cache="true" dataPath="${path2}"/>
    </sources>

    <!-- 基于 sources 中定义的基础数据进行的转换、清洗操作；
	支持向上依赖 -->
    <transforms>
        <transform tableName="people_t" cache="true" save="true" type="sql">
            <![CDATA[
            select job, day, age, education from people1
            union all
            select job, day, age, education from people2
            ]]>
        </transform>

        <transform tableName="people_t2" cache="true" save="true" type="sql">
            <![CDATA[
            select job, day, age, education from people1
            union all
            select job, day, age, education from people2
            union all
            select job, day, age, education from people_t
            ]]>
        </transform>

        <transform tableName="people_t3" cache="true" save="true" type="sql">
            <![CDATA[
            select job, day, age, education from people1
            union all
            select job, day, age, education from people_t2
            ]]>
        </transform>
    </transforms>

    <!-- 基于基础数据转换、清洗完成后，需要最终落地的数据 -->
    <sinks>
        <sink tableName="people_t" type="parquet" saveMode="overwrite" savePath="${path3}"/>
        <sink tableName="people_t2" type="txt" partitions="1" delimiter=" || " saveMode="overwrite" savePath="${path4}"/>
        <sink tableName="people_t3" type="txt" coalesce="2" delimiter=" || " saveMode="overwrite" savePath="${path5}"/>
    </sinks>
</logic>
```

### 数据源定义

部分参数说明：

**tableName:** 数据源读取后在 Spark-SQL 中的注册表名

**type:** 数据源的类别，csv、txt、parquet等

**partitions:** 数据读取后重新分区的数量

**cache:** 是否缓存表

**dataPath:** 数据存放路径，默认使用 HDFS 路径

**schemaPath:** 当数据源为 txt 类的文本文件时，需要指定数据的 schema，比如：列分隔符、列名、列值类型等定义

```xml
<!-- 用于后续转换、清洗等操作的基础数据 -->
<sources>
    <source tableName="people1" type="csv" partitions="1" cache="true" dataPath="${path1}" schemaPath="/path/to/people.meta"/>
    <source tableName="people2" type="parquet" partitions="1" cache="true" dataPath="${path2}"/>
</sources>
```

**people.meta**

> 文本类数据源的结构映射定义，主要定义有数据的编码格式、列分隔符、每列的列名、列值类型等

```xml
<?xml version="1.0" encoding="UTF-8"?>
<source charset="UTF-8" delimiter="\|\|" isHeaderFirst="true">
    <columns>
        <column type="STRING" nullable="true" index="1"> age </column>
        <column type="STRING" nullable="true" index="2"> job </column>
        <column type="STRING" nullable="true" index="3"> marital </column>
        <column type="STRING" nullable="true" index="4"> education </column>
        <column type="STRING" nullable="true" index="5"> default </column>
        <column type="STRING" nullable="true" index="6"> balance </column>
        <column type="STRING" nullable="true" index="7"> housing </column>
        <column type="STRING" nullable="true" index="8"> loan </column>
        <column type="STRING" nullable="true" index="9"> contact </column>
        <column type="STRING" nullable="true" index="10"> day </column>
        <column type="STRING" nullable="true" index="11"> month </column>
        <column type="STRING" nullable="true" index="12"> duration </column>
        <column type="STRING" nullable="true" index="13"> campaign </column>
        <column type="STRING" nullable="true" index="14"> pdays </column>
        <column type="STRING" nullable="true" index="15"> previous </column>
        <column type="STRING" nullable="true" index="16"> poutcome </column>
        <column type="STRING" nullable="true" index="17"> y </column>
    </columns>
</source>
```

### 清洗定义

基于上面来源数据表定义，进行我们期望的数据转换、清洗操作。

部分参数说明：

**tableName:** SQL执行完成后的数据注册成的表名

**cache:** 是否缓存表

**save:**  是否对此中间表存储

**type**: 当前只支持sql

```xml
<!-- 基于 sources 中定义的基础数据进行的转换、清洗操作；
支持向上依赖 -->
<transforms>
    <transform tableName="people_t" cache="true" save="true" type="sql">
        <![CDATA[
        select job, day, age, education from people1
        union all
        select job, day, age, education from people2
        ]]>
    </transform>

    <transform tableName="people_t2" cache="true" save="true" type="sql">
        <![CDATA[
        select job, day, age, education from people1
        union all
        select job, day, age, education from people2
        union all
        select job, day, age, education from people_t
        ]]>
    </transform>

    <transform tableName="people_t3" cache="true" save="true" type="sql">
        <![CDATA[
        select job, day, age, education from people1
        union all
        select job, day, age, education from people_t2
        ]]>
    </transform>
</transforms>
```

### 数据持久化

对于基于 sources 层和 transforms 层得到的表中最终需要持久化的表进行持久化操作，也就是整个逻辑的结果持久化。

部分参数说明：

**tableName:** sources和transforms中需要持久化的表

**type:** 持久化时数据类型，csv、txt、parquet 等

**saveMode:**  持久化动作，append、overwrite 等

**partitions:** 分区数，即生成的文件数

**partitionBy:** 分区字段

```xml
<!-- 基于基础数据转换、清洗完成后，需要最终落地的数据 -->
<sinks>
    <sink tableName="people_t" type="parquet" saveMode="overwrite" savePath="${path3}"/>
    <sink tableName="people_t2" type="txt" partitions="1" delimiter=" || " saveMode="overwrite" savePath="${path4}"/>
    <sink tableName="people_t3" type="txt" coalesce="2" delimiter=" || " saveMode="overwrite" savePath="${path5}"/>
</sinks>
```

### 执行

通过 spark-submit 执行进行提交即可

```shell
spark-submit \
--jars ... \
--class com.moon.Run frog.jar \
--transform-file ./transform.xml
```

## 开发待完善