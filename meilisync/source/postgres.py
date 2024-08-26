import asyncio
import json
import uuid
from asyncio import Queue
from typing import List, Any

import psycopg2
import psycopg2.errors
from psycopg2.extras import LogicalReplicationConnection, RealDictRow, RealDictCursor
from psycopg2._psycopg import ReplicationMessage

from meilisync.enums import EventType, SourceType
from meilisync.schemas import Event, ProgressEvent
from meilisync.settings import Sync
from meilisync.source import Source

# Connection pooling
from psycopg2.pool import ThreadedConnectionPool

class CustomDictRow(psycopg2.extras.RealDictRow):
    def __getitem__(self, key):
        try:
            return super().__getitem__(key)
        except KeyError as exc:
            if isinstance(key, int):
                return super().__getitem__(list(self.keys())[key])
            raise exc


class CustomDictCursor(psycopg2.extras.RealDictCursor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        kwargs["row_factory"] = CustomDictRow
        self.row_factory = CustomDictRow


class Postgres(Source):
    type = SourceType.postgres
    slot = "meilisync"
    _pool = None
    
    def __init__(self, progress: dict, tables: List[str], **kwargs):
        super().__init__(progress, tables, **kwargs)
        if not Postgres._pool:
            Postgres._pool = ThreadedConnectionPool(
                1, 
                10,
                connection_factory=LogicalReplicationConnection, 
                **self.kwargs
                )
        self.conn = Postgres._pool.getconn()
        self.cursor = self.conn.cursor()
        self.queue = None  
           
        if self.progress:
            self.start_lsn = self.progress["start_lsn"]
        else:
            self.cursor.execute("SELECT pg_current_wal_lsn()")
            self.start_lsn = self.cursor.fetchone()[0]
        self.conn_dict = Postgres._pool.getconn()

    async def get_current_progress(self):
        sql = "SELECT pg_current_wal_lsn()"

        def _():
            with self.conn.cursor() as cur:
                cur.execute(sql)
                ret = cur.fetchone()
                return ret[0]

        start_lsn = await asyncio.get_event_loop().run_in_executor(None, _)
        return {"start_lsn": start_lsn}

    async def get_full_data(self, sync: Sync, size: int):
        if sync.fields:
            fields = ", ".join(f"\"{field}\" as \"{sync.fields[field] or field}\"" for field in sync.fields)
        else:
            fields = "*"
        offset = 0

        def _():
            with self.conn_dict.cursor() as cur:
                cur.execute(
                    f"SELECT {fields} FROM \"{sync.table}\" ORDER BY "
                    f"\"{sync.pk}\" LIMIT {size} OFFSET {offset}"
                )
                return cur.fetchall()

        while True:
            ret = await asyncio.get_event_loop().run_in_executor(None, _)
            if not ret:
                break
            offset += size
            yield ret

    def _consumer(self, msg: ReplicationMessage):
        payload = json.loads(msg.payload)
        next_lsn = payload["nextlsn"]
        changes = payload.get("change", [])

        for change in changes:
            self.__handle_change(change, next_lsn)

        msg.cursor.send_feedback(flush_lsn=msg.data_start)

    def __handle_change(self, change: dict[str, Any], next_lsn: str):
        table = change.get("table")
        if table not in self.tables:
            return

        columnnames = change.get("columnnames", [])
        columnvalues = change.get("columnvalues", [])
        columntypes = change.get("columntypes", [])

        for i in range(len(columntypes)):
            if columntypes[i] == "json":
                columnvalues[i] = json.loads(columnvalues[i])

        kind = change.get("kind")
        if kind == "update":
            values = dict(zip(columnnames, columnvalues))
            event_type = EventType.update
        elif kind == "delete":
            values = (
                dict(zip(columnnames, columnvalues))
                if columnvalues
                else {change["oldkeys"]["keynames"][0]: change["oldkeys"]["keyvalues"][0]}
            )
            event_type = EventType.delete
        elif kind == "insert":
            values = dict(zip(columnnames, columnvalues))
            event_type = EventType.create
        else:
            return

        # Queueing the events in bulk for better performance
        event = Event(
            type=event_type,
            table=table,
            data=values,
            progress={"start_lsn": next_lsn},
        )
        
        asyncio.create_task(self.queue.put(event))

    async def __aiter__(self):
        self.queue = Queue()
        
        def slot_exists():
            self.cursor.execute(f"SELECT slot_name FROM pg_replication_slots WHERE slot_name = '{self.slot}'")
            return self.cursor.fetchone() is not None
        
        if not await asyncio.get_event_loop().run_in_executor(None, slot_exists):
            self.cursor.create_replication_slot(self.slot, output_plugin="wal2json")
            
        def slot_in_use():
            self.cursor.execute(f"SELECT active_pid FROM pg_replication_slots WHERE slot_name = '{self.slot}'")
            result = self.cursor.fetchone()
            return result is not None and result[0] is not None
    
        if await asyncio.get_event_loop().run_in_executor(None, slot_in_use):
            print(f"Slot {self.slot} is currently in use. Exiting...")
            return

        self.cursor.start_replication(
            slot_name=self.slot,
            decode=True,
            status_interval=1,
            start_lsn=self.start_lsn,
            options={
                "include-lsn": "true",
                "include-types" : "true",
                "include-pk" : "true",
            },
        )
        
        asyncio.ensure_future(
            asyncio.get_event_loop().run_in_executor(
                None, self.cursor.consume_stream, self._consumer
            )
        )
            
        yield ProgressEvent(progress={"start_lsn": self.start_lsn})
        
        while True:
            yield await self.queue.get()
            
        
    def _ping(self):
        with self.conn_dict.cursor() as cur:
            cur.execute("SELECT 1")

    async def ping(self):
        await asyncio.get_event_loop().run_in_executor(None, self._ping)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        Postgres._pool.putconn(self.conn)
        Postgres._pool.putconn(self.conn_dict)

