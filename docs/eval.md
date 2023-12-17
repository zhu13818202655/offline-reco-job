# 后评估参考文档

## 用户池划分

1. 实验组(user_type=exp)，算法选人，算法选产品，`90000人`
2. 实验对照组(user_type=exp1)，算法选人，随机选产品，`5000人`
3. 头部空白组(user_type=ctl1)，算法选人，不推产品，`5000人`
3. 对照组(user_type=ctl)，随机选人，随机选产品，`5000人`
4. 空白组(user_type=noop)，不参与投放， `5000人`

划分规则：

- 每天动态更新到 `xianzhi.bdasire_card_user_pool` 中，T日投放，T-1日计算，存入分区 dt=T-2（数据日期）
- 类别继承：
  - 如果今天用户出现在昨天的划分中，则继承昨天的划分结果
    - 如果用户非实验组，也会继承昨天的分数
  - 如果今天用户没有出现在昨天的划分中，则按比例随机切分

## 线上回收任务

### 人群反馈任务 CrowdFeedback 


输出表名：`xianzhi.bdasire_card_crowd_feedback`


| name         | type   | comment                                       | is partition |
|--------------|--------|-----------------------------------------------|--------------|
| `user_id`    | string | 用户名                                        | NO           |
| `user_type`  | string | 用户类型                                      | NO           |
| `item_id`    | string | 产品编号                                      | NO           |
| `channel_id` | string | 渠道编号                                      | NO           |
| `banner_id`  | string | 栏位编号                                      | NO           |
| `send`       | int    | 用户回收当天的触达次数，0或1                  | NO           |
| `click`      | int    | 用户回收当天的点击次数，短信无法追踪，为空    | NO           |
| `convert`    | string | 用户回收当天的转化指标，json string，暂时为空 | NO           |
| `batch_id`   | string | 跑批时间                                      | NO           |
| `dt`         | string | 日期分区                                      | YES          |
| `dag_id`     | string | 产品线编号                                    | YES          |


要点：

- 日更，每个分区存当天的回收结果
- 短信、手机银行的回收都放在这张表，用 channel_id 区分
  - 短信: `channel_id=PSMS_IRE`，`banner_id` 为空
  - 手机银行：`channel_id=PMBS_IRE`, 白名单`banner_id=WHITELIST`
- 分区为投放结果计算时的数据的分区，举个例子：
  - 短信：`20220102` 日计算产出人群包，数据分区 `dt=20220101` ，`20220103` 日给下游并投放，`20220106` 日跑回收作业，回收结果存到 `xianzhi.bdasire_card_crowd_feedback` 的分区 `20220101` 中
  - 手机银行：`20220102` 日计算产出人群包，数据分区 `dt=20220101` ，`20220103` 日给下游，`20220104`投放，`20220105` 日跑回收作业，回收结果存到 `xianzhi.bdasire_card_crowd_feedback` 的分区 `20220101` 中



### [已废弃] 短信用户行为特征回收 DailyReportSMS

输出表名：`xianzhi.bdasire_card_attr_feedback`


| name                                    | type   | comment                    | is partition |
|-----------------------------------------|--------|----------------------------|--------------|
| `user_id`                               | string | 用户名                     | NO           |
| `user_type`                             | string | 用户类型                   | NO           |
| `send`                                  | string | 用户当月累计触达次数，0或1 | NO           |
| `batch_id`                              | string | 跑批时间                   | NO           |
| `dt_sub1`                               | string | 短信触达时间前一天         | NO           |
| `if_hold_card`                          | string | 是否持卡                   | NO           |
| `fenhang`                               | string | 分行                       | NO           |
| `pre_tot_asset_n_zhixiao_month_avg_bal` | float  |                            | YES          |
| `tot_asset_n_zhixiao_month_avg_bal`     | float  |                            | YES          |
| `dt`                                    | string | 日期分区                   | YES          |
| `dag_id`                                | string | 产品线编号                 | YES          |


要点：

- 日更，每个分区存当月累计回收结果
- 分区设定规则同上文人群反馈表
- 该表的生成依赖人群反馈表，计算时会选择：
  - 当月所有短信渠道触达的实验组、对照组、实验对照组（send>0）
  - 当月的空白组、头部空白组（send=0）
