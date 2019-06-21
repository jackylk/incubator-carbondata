# Carbon AI演示方案

演示的受众分为**大数据分析师**和**AI开发工程师**。



### Demo1：面向大数据分析师，寻找“颜值最高的明星”

- 使用Leo之前：只通过结构化数据分析，没有利用非结构化数据。如果想用图片数据，则需要大量编程，并自己解决分布式问题。

- 使用Leo之后：不需要编程，快速利用网上的图片和文本数据进行分析，包括数据爬取、调用EI能力、统计分析。

演示步骤：

1. 收集网上数据，从搜索引擎中得到图片路径和标题。建立table t

   ```sql
   create table t (
     string path, 
     string title,
     int rank) 
   as 
   	select web_search('baidu_img', '颜值最高的明星', 100);
   ```

   

2. 爬取图片，并将图片入库table t

   ```sql
   insert columns (
     img binary, 
     width int, 
     height int, 
     channel int) 
   into t 
   as 
   	select 
       image_content(download(path)), 
       image_width(download(path)), 
       image_height(download(path)), 
       image_channel(download(path)) 
   	from t;
   ```

   

3. 调用*命名实体API*，从title中得到明星名字

   ```sql
   insert columns (
     name_from_title auto_struct) 
   into t
   as 
   	select ei_named_entity(title) 
   	from t;
   ```

   

4. 调用*名人识别API*，从图片中得到明星名字

   ```sql
   insert columns (
   	name_from_image auto_struct)
   into t
   as
   	select ei_celebrity_recognition(base64(img))
   	from t;
   ```

   

5. 数据清洗，过滤出上两步名字相同的人。统计数量、按排名显示图片

   ```sql
   -- 统计人数，剔除不干净的数据
   select count(*) 
   from t 
   where name_from_title.name = name_from_image.name;
   
   -- 显示颜值最高的10个人
   !display(
   	"select img 
       from t 
       where name_from_title.name = name_from_image.name
       order by rank
       limit 10",
   	10)
   ```



### Demo2：面向AI开发工程师，开发“笑vs哭”表情分类模型

- 使用Leo之前：需要大量手动+编程，手动爬取、清洗、调用REST、并自己解决分布式问题。

- 使用Leo之后：不需要编程，快速爬取网上图片，进行清洗、调用ModelArts进行训练、部署推理服务。

演示步骤：（基于第一个演示继续在table t上进行）

1. 采集爬取”哭泣”的图片

   ```sql
   create table t (
     string path, 
     string title,
     int rank) 
   as 
   	select web_search('baidu_img', '哭泣的人', 100);
   ```

   

2. 打标签。1代表微笑，0代表哭泣。已有的明星图片都是微笑的，所以是1。当前demo是直接插入标签值，未来和ModelArts系统联合做标注

   ```sql
   insert columns (label string) 
   into t
   as 
   	select 
       case when name_from_image != null then '1' 
       else '0' 
       end
   	from t;
   ```

   

3. 训练模型

   ```sql
   create model db1.model1
   options (
   	'type' = 'tensorflow/image_classification',
   	'base_model' = 'inceptorv3',
   	'label_column' = 'label',
   	'max_iteration' = 100)
   as
   	select img, label 
   	from t
   ```



4. 推理。先将模型注册为udf，收集另一些数据来用于做推理

   ```sql
   register model db1.model1
   as expression_classification;
   
   -- 收集要推理的图片
   create table p (
     string path, 
     string title,
     int rank) 
   as 
   	select web_search('baidu_img', '特朗普', 100);
   	
   -- 爬取图片
   insert columns (
     img binary, 
     width int, 
     height int, 
     channel int) 
   into p
   as 
   	select 
       image_content(download(path)), 
       image_width(download(path)), 
       image_height(download(path)), 
       image_channel(download(path)) 
   	from p;
   	
   -- 调用udf进行表情识别
   insert columns (expression int)
   into p
   as 
   	select expression_classification(base64(img))
   	from p;
   
   ```

   

5. 停止推理，删除模型

   ```sql
   unregister model db1.model1;
   
   drop model db1.model1;
   ```

   
