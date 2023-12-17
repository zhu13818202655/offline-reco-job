from collections import defaultdict
from enum import Enum
from typing import Iterable, Tuple, Type, TypeVar, Union

from core.job.base import Job
from core.logger import tr_logger


class JobType(Enum):
    BASE = 0
    INFER = 1  # 算法模块
    FEEDBACK = 2  # 回收模块
    REPORT = 3  # 报表模块


class JobNode:
    def __init__(self, clz, type_, *upstreams: Union[Type[Job], str], **meta):
        self.clz = clz
        self.type_ = type_
        self.upstreams = [i.__name__ if isinstance(i, type) else i for i in upstreams]
        self.meta = meta


# Job类泛型
_Job = TypeVar('_Job')


class JobRegistry:

    _registry = defaultdict(dict)

    T = JobType

    @classmethod
    def register(cls, t: JobType, *upstreams: Union[Type[Job], str], **meta):

        registry = cls._registry[t]

        def _register(job_cls: Type[_Job]) -> Type[_Job]:
            name = job_cls.__name__
            assert issubclass(job_cls, Job), f'{name} is not subclass of engine.Job'
            if name not in registry:
                registry[name] = JobNode(job_cls, t, *upstreams, **meta)
                tr_logger.info(f'job class: {name} registered!')
            return job_cls

        return _register

    @classmethod
    def iter_all_jobs(cls) -> Iterable[Tuple[str, JobNode]]:
        for k in cls._registry:
            for job in cls._registry[k]:
                yield job, cls._registry[k][job]

    @classmethod
    def list_jobs(cls, t: JobType):
        return [node.clz for node in cls._registry[t].values()]

    @classmethod
    def list_job_names(cls, t: JobType):
        return list(cls._registry[t].keys())

    @classmethod
    def lookup(cls, name) -> JobNode:
        for k in cls._registry:
            for job_name in cls._registry[k]:
                if job_name == name:
                    return cls._registry[k][name]
        tr_logger.error(f'job name {name} not registered!')
        raise ModuleNotFoundError(f'job name {name} not registered!')
