# -*- coding: utf-8 -*-
# @File : integration_test.py
# @Author : r.yang
# @Date : Mon May 16 10:13:24 2022
# @Description :


def doc():
    return """
    集成测试在行方 UAT 环境完成

    检查以下要点：

    〇、推荐作业

    - [ ] 模型训练 dag 触发，是否正常产出模型
    - [ ] base 作业成功执行
    - [ ] user_pool 产出信用卡和借记卡用户人数符合预期
    - [ ] operating_user 各个渠道、随机组对照组输出人数是否符合预期
    - [ ] operating_user 最终挑选出来的人是否符合业务逻辑：
        - 分数高的被选中
        - 已经投放过的不会被选中
    - [ ] crowd_feedback 测试环境无反馈数据，无法验证，跑通即可
    - [ ] crowd_package 产出的各渠道、随机组对照组人数是否符合预期
    - [ ] crowd_package 的产品ID是否和模型分布一致

    一、渠道作业

    - [ ] 短信渠道手机号、正文是否正常
    - [ ] 短信渠道当天和前三天名单交集是否为空
    - [ ] 白名单渠道当天和昨天名单交集重合率是否大于90%
    - [ ] 短信&白名单渠道是否每个用户只推荐一个产品
    - [ ] 手机银行渠道是否每个用户推荐N个产品（N为槽位数）
    - [ ] 渠道数据量是否和 crowd_package 结果一致
    - [ ] 渠道是否有测试数据，数量是否符合预期

    """
