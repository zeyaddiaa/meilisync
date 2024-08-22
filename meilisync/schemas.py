import datetime
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel

from meilisync.enums import EventType


class ProgressEvent(BaseModel):
    progress: dict | None = None


class Event(ProgressEvent):
    type: EventType
    table: str | None = None
    data: dict

    def mapping_data(self, fields_mapping: Optional[dict] = None):
        data = {}
        for k, v in self.data.items():
            if isinstance(v, datetime.datetime):
                if v.year > 0:
                    v = int(v.timestamp())
                else:
                    v = 1
            elif isinstance(v, datetime.date):
                v = str(v)
            elif isinstance(v, Decimal):
                v = float(v)
            if fields_mapping is not None and k in fields_mapping:
                real_k = fields_mapping[k] or k
                data[real_k] = v
            elif fields_mapping is None:
                data[k] = v
        return data or self.data
