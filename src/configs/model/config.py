# -*- coding: utf-8 -*-
# @File : scene.py
# @Author : r.yang
# @Date : Tue Jan 11 14:08:56 2022
# @Description : scene config


from typing import Dict, List, Optional

from pydantic.main import BaseModel

from configs.model.base import Activity, BaseConfig, ClassifierConfig, Selector, TrainConfig
from configs.utils import ConfigRegistry, UserType


class RecoJobConfig(BaseModel):
    pass


class RankingConfig(RecoJobConfig):

    model: ClassifierConfig

    force_rank_custs: List[str] = []


class UserPoolConfig(RecoJobConfig):

    filters: Dict[str, str] = {}
    user_type_refresh_days: List[int]
    model: ClassifierConfig


class GroupInfo(BaseModel):
    num: int  # 该组最终投放的人数

    # 是否只需要 num 数目的人即可满足要求
    # - True: 分配时会之只分配 num 数量的人（比如空白组，对照组）防止污染实验组导致效果不好
    # - False: 分配时会尽可能多的按比例分，根据总人数来决定（比如实验组）
    minimum: bool = False


class ChannelOperateConfig(BaseModel):

    channel_id: str
    banner_id: str = ''

    # Tuple[投放人数、圈选比例]
    group_users: Dict[str, GroupInfo]
    # 空白组不参与计算
    groups_no_push: List[UserType] = [UserType.CTL1, UserType.NOOP]

    items_specified: List[str] = []

    class Config:
        use_enum_values = True


class OperatingUserConfig(RecoJobConfig):

    channel_cfgs: List[ChannelOperateConfig]
    fail_rate: float = 0.01

    def get_channel_cfg(self, channel_id: str, banner_id: str = '') -> ChannelOperateConfig:
        for cfg in self.channel_cfgs:
            if cfg.channel_id == channel_id and cfg.banner_id == banner_id:
                return cfg
        raise KeyError(f'channel_id: {channel_id}, banner_id: {banner_id}')


class CrowdFeedbackConfig(RecoJobConfig):
    popup_identifiers: List[str] = []
    # 因为测试环境拿不到回收数据，只能假设全部投放成功，用于测试
    sms_default_send_num: int

    user_agg_start_dt: str
    user_agg_days: int


class ReportConfig(RecoJobConfig):

    stat_periods_in_day: List[int]


class AdhocSceneConfig(RecoJobConfig):
    s1106_base_product: str
    s1108_base_product: str

    s1106_max_prod_num: int
    s1108_max_prod_num: int
    s1109_max_prod_num: int

    redis_key_ttl_ex: int = 15 * 3600 * 24  # 默认15天超时


@ConfigRegistry.register()
class SceneConfig(BaseModel):
    scene_id: str
    scene_name: str

    user_pool: Optional[UserPoolConfig] = None
    ranking: Optional[RankingConfig] = None
    operating_user: Optional[OperatingUserConfig] = None
    crowd_feedback: Optional[CrowdFeedbackConfig] = None
    report: Optional[ReportConfig] = None

    train: TrainConfig

    item_selector: Selector

    adhoc: Optional[AdhocSceneConfig] = None


@ConfigRegistry.register()
class RecoDagConfig(BaseModel):
    dag_id: str
    dag_name: str

    scene: SceneConfig
    base: Optional[BaseConfig] = None


class PostProcJobConfig(BaseModel):
    pass


class SMSConfig(PostProcJobConfig):

    push_days: List[int]


class APPConfig(PostProcJobConfig):
    pass


class PushConfig(PostProcJobConfig):
    pass


@ConfigRegistry.register()
class PostProcessDagConfig(BaseModel):
    dag_id: str
    dag_name: str

    channel_id: str
    actvs: List[Activity]

    base: Optional[BaseConfig] = None

    sms: Optional[SMSConfig] = None
    app: Optional[APPConfig] = None
    push: Optional[PushConfig] = None


@ConfigRegistry.register()
class BaseDagConfig(BaseModel):
    dag_id: str
    dag_name: str

    base: Optional[BaseConfig] = None
