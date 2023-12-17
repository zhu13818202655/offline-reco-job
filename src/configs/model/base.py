# -*- coding: utf-8 -*-
# @File : base.py
# @Author : r.yang
# @Date : Tue Jan 11 13:56:31 2022
# @Description :
import base64
import datetime
import os
from typing import Any, Dict, List, Optional, Union

from dateutil.relativedelta import relativedelta
from pydantic import BaseModel, Field, validator

from configs.model.table import ExternalTable, OutputTable
from configs.utils import ClassifierRegistry, ConfigRegistry, Env, ProdType


class FeatureEngineConfig(BaseModel):
    feat_dir: str = ''  # to be filled by baseconfig
    feat_version: str
    feat_map: Dict[str, List[Any]]
    user_feat_tables: List[str] = []
    item_feat_tables: List[str] = []

    train_dt: str


class TrainConfig(BaseModel):

    train_sample_month_num: int
    eval_sample_month_num: int
    sample_dt: Optional[str] = Field(
        default_factory=lambda: '20220301' if Env.is_stage or Env.is_local else None
    )

    @property
    def _sample_dt(self) -> datetime.datetime:
        if self.sample_dt is None:
            return datetime.datetime.today()
        return datetime.datetime.strptime(self.sample_dt, '%Y%m%d')

    @property
    def sample_prt(self) -> str:
        """训练数据日期分区，取上个月的最后一天
        """
        if Env.is_local:
            return '20220101'

        dt = datetime.datetime(year=self._sample_dt.year, month=self._sample_dt.month, day=1)
        dt = dt + datetime.timedelta(days=-1)
        return dt.strftime('%Y%m%d')

    @property
    def train_sample_month(self) -> List[str]:
        """根据 sample_dt 往前取对应长度的月份作为训练集
        """

        train_months = []
        for i in range(self.train_sample_month_num + self.eval_sample_month_num):
            sample_dt = self._sample_dt + relativedelta(months=-(i + 1))
            train_months.append(sample_dt.strftime('%Y%m'))

        return train_months[-self.train_sample_month_num :]

    @property
    def eval_sample_month(self):
        """根据 sample_dt 往前取对应长度的月份作为验证集
        """
        eval_months = []
        for i in range(self.eval_sample_month_num):
            sample_dt = self._sample_dt + relativedelta(months=-(i + 1))
            eval_months.append(sample_dt.strftime('%Y%m'))

        return eval_months


class ItemInfo(BaseModel):

    item_ids: List[str] = []
    desc: str
    # 训练标签，从0开始
    # [ADHOC] -1 表示不训练，如果是新产品，数据较少，不应当进训练
    label: int
    type: ProdType

    prop: float = 0.0  # 强制切分流量比重

    sms_content: str = ''

    enable: bool = True

    class Config:
        use_enum_values = True


class UserInfo(BaseModel):
    user_id: str
    phone_no: str
    scope: str = 'all'


class ChannelInfo(BaseModel):
    channel_id: str
    channel_name: str = ''

    banner_id: str = ''

    # 回收延迟，=1 意味着 T 日投出去，T+1日早上6点回收结果入仓
    feedback_delay: int = 1
    # 推送延迟，=1 意味着 T 日BI收到，T+1日投放
    push_delay: int
    max_push: int  # 每个月最大推送数目
    allow_repeat: bool  # 是否每次可以推送一样的item
    num_items: int  # 每次推送的 item 数目


Selector = Dict[str, Union[str, int, List[str], List[int]]]


class SceneInfo(BaseModel):
    scene_id: str
    scene_name: str = ''
    prod_type: ProdType
    item_selector: Optional[Selector]

    class Config:
        use_enum_values = True


class LowcodeConfig(BaseModel):
    url: str
    secret: str
    client_id: str


class RedisConfig(BaseModel):
    addrs: List[str] = []
    master_name: str = ''
    password: str = ''
    db: int = 10

    @classmethod
    def from_env(cls):
        addrs = []
        if 'REDIS_HOST' not in os.environ:
            return cls()
        host = os.environ['REDIS_HOST']
        port = int(os.environ['REDIS_PORT'])
        addrs.append(f'{host}:{port}')

        master_name = ''
        password = base64.b64decode(os.environ['REDIS_PASSWORD']).decode()
        db = int(os.environ.get('REDIS_DBNUM2', 10))

        if 'REDIS_HOST2' in os.environ and 'REDIS_HOST3' in os.environ:
            addrs.append(f'{os.environ["REDIS_HOST2"]}:{os.environ["REDIS_PORT2"]}')
            addrs.append(f'{os.environ["REDIS_HOST3"]}:{os.environ["REDIS_PORT3"]}')
            if 'MASTER_NAME' in os.environ:
                master_name = os.environ['MASTER_NAME']
            else:
                master_name = os.environ['AIRFLOW__CELERY_BROKER_TRANSPORT_OPTIONS__MASTER_NAME']

        return cls(addrs=addrs, master_name=master_name, password=password, db=db)


@ConfigRegistry.register()
class BaseConfig(BaseModel):

    react_delay: int
    data_keep_day: int
    # 默认 hive 保存 90 天数据
    partition_keep_day: int

    output_table: OutputTable
    external_table: ExternalTable

    hdfs_output_dir: str
    nas_output_dir: str
    feat_dir: str
    offline_backend_address: str
    online_backend_address: str
    lowcode: LowcodeConfig
    redis: RedisConfig

    feat_engines: List[FeatureEngineConfig]

    channels: List[ChannelInfo]
    scenes: List[SceneInfo]

    # 只加到场景列表接口中，不产出数据
    test_banners: List[str]

    items: List[ItemInfo]
    test_users: List[UserInfo]

    @validator('feat_engines', always=True, each_item=True)
    def validate_feat_engines(cls, value, values):
        ext_tables = values['external_table'].dict()
        for k in ext_tables:
            if k.startswith('user_feat_') and ext_tables[k] not in value.user_feat_tables:
                value.user_feat_tables.append(ext_tables[k])

            if k.startswith('item_feat_') and ext_tables[k] not in value.item_feat_tables:
                value.item_feat_tables.append(ext_tables[k])
            value.feat_dir = values['feat_dir']
        return value

    def get_channel(self, channel_id, banner_id=''):
        for c in self.channels:
            if c.channel_id == channel_id and c.banner_id == banner_id:
                return c
        raise KeyError(f'channel: {channel_id}, banner: {banner_id} not exist')

    def get_scene(self, scene_id):
        for s in self.scenes:
            if s.scene_id == scene_id:
                return s
        raise KeyError(f'scene: {scene_id} not exist')

    class Config:
        # 每次 assignment 时都会触发 validate，保证 feat_engine.user_feat_tables 有值
        validate_assignment = True


class UserItemMatcher(BaseModel):
    user_selector: Selector
    item_selector: Selector


class Activity(BaseModel):
    actv_id: str
    scene_id: str

    banner_id: str = ''

    # 和活动绑定的配置
    phone_no_field: str = 'credit_crd_phone_no'  # 该活动用哪个手机号字段
    test_user_item: Optional[UserItemMatcher] = None
    to_BI: bool = True  # 是否在 BI 的场景列表中展示


class ClassifierConfig(BaseModel):
    model_dir: str
    model_version: str
    feat_version: str

    model_clz: str
    custom_train_args: dict = {}

    @validator('model_clz', always=True)
    def validate_model_clz(cls, value, values):  # pylint: disable=E0213
        import algorithm.classifer  # noqa

        try:
            ClassifierRegistry.lookup(value)
        except KeyError:
            value = 'XGBClassifierModel'
        return value

    @validator('model_version', always=True)
    def validate_model_version(cls, value, values):  # pylint: disable=E0213
        if value.count('-') > 1:
            raise ValueError(f'you can only have ONE slash in model_version')
        return value

    @property
    def train_args(self):
        clz = ClassifierRegistry.lookup(self.model_clz)
        default_args = clz.default_train_args()
        return dict(default_args, **self.custom_train_args)

    @property
    def model_file(self):
        clz = ClassifierRegistry.lookup(self.model_clz)
        return clz.model_file_name()

    @property
    def sample_version(self):
        return self.model_version.split('-')[0]
