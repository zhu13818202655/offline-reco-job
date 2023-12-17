from typing import Dict, List, Optional, Union

from pydantic import BaseModel, Field

RecordValueType_ = Union[str, int, float, bool]
DictValueType_ = Union[RecordValueType_, List[RecordValueType_]]


class RecordValue(BaseModel):
    name: str
    value: RecordValueType_ = None


def parse_values_to_dict(values: List[RecordValue],) -> Dict[str, DictValueType_]:
    d: Dict[str, DictValueType_] = {}
    for o in values:
        if o.name in d:
            if isinstance(d[o.name], list):
                d[o.name].append(o.value)
            else:
                d[o.name] = [d[o.name], o.value]
        else:
            d[o.name] = o.value
    return d


class LowCodeNamespace(BaseModel):
    namespace_id: str = Field(..., alias='namespaceID')
    name: str = Field(..., alias='name')
    handle: str = Field(..., alias='slug')


class LowCodeModule(BaseModel):
    namespace_id: str = Field(..., alias='namespaceID')
    module_id: str = Field(..., alias='moduleID')
    name: str = Field(..., alias='name')
    handle: str = Field(..., alias='handle')


class LowCodeRecord(BaseModel):
    record_id: str = Field(..., alias='recordID')
    module_id: str = Field(..., alias='moduleID')
    values: List[RecordValue]
    namespace_id: str = Field(..., alias='namespaceID')
    owned_by: str = Field(..., alias='ownedBy')
    created_at: Optional[str] = Field(None, alias='createdAt')
    created_by: Optional[str] = Field(None, alias='createdBy')
    updated_at: Optional[str] = Field(None, alias='updatedAt')
    updated_by: Optional[str] = Field(None, alias='updatedBy')
    can_update_record: Optional[bool] = Field(None, alias='canUpdateRecord')
    can_read_record: Optional[bool] = Field(None, alias='canReadRecord')
    can_delete_record: Optional[bool] = Field(None, alias='canDeleteRecord')
    can_grant: Optional[bool] = Field(None, alias='canGrant')

    def get_value(self, key):
        v = None
        vlist = []
        for _ in self.values:
            if _.name != key:
                continue
            if v is None:
                v = _.value
            else:
                if not vlist:
                    vlist.append(v)
                vlist.append(_.value)
        if vlist:
            return vlist
        if v is None:
            raise KeyError(f"Can't find value for'{key}'")
        return v


ApiReturnType = Dict[str, Union[Union[Dict, LowCodeRecord], List[Union[Dict, LowCodeRecord]]]]


class ApiError(Exception):
    def __init__(self, message: str):
        self.message = message

    def __str__(self):
        return self.message
