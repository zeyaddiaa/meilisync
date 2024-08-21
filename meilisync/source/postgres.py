import asyncio
import json
from asyncio import Queue
from typing import List, Any

import psycopg2
import psycopg2.errors
from psycopg2._psycopg import ReplicationMessage
from psycopg2.extras import LogicalReplicationConnection

from meilisync.enums import EventType, SourceType
from meilisync.schemas import Event, ProgressEvent
from meilisync.settings import Sync
from meilisync.source import Source

import uuid

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

    def __init__(
        self,
        progress: dict,
        tables: List[str],
        **kwargs,
    ):
        super().__init__(progress, tables, **kwargs)
        self.conn = psycopg2.connect(**self.kwargs, connection_factory=LogicalReplicationConnection)
        self.cursor = self.conn.cursor()
        self.queue = None
        self.slot = self._generate_unique_slot_name()
        
        if self.progress:
            self.start_lsn = self.progress["start_lsn"]
        else:
            self.cursor.execute("SELECT pg_current_wal_lsn()")
            self.start_lsn = self.cursor.fetchone()[0]
        self.conn_dict = psycopg2.connect(**self.kwargs, cursor_factory=CustomDictCursor)

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
            
    def _generate_unique_slot_name(self):
        base_slot_name = f"meilisync_{uuid.uuid4().hex}"
        slot_name = base_slot_name

        # Check if slot already exists and is active
        while True:
            self.cursor.execute(f"SELECT active_pid FROM pg_replication_slots WHERE slot_name = '{slot_name}'")
            slot_info = self.cursor.fetchone()

            if slot_info and slot_info[0]:  # Slot is active
                print(f"Slot {slot_name} is already in use by PID {slot_info[0]}, generating a new slot name.")
                slot_name = f"meilisync_{uuid.uuid4().hex}"  # Generate a new unique slot name
            else:
                print(f"Using slot name {slot_name}.")
                return slot_name
            
    def _consumer(self, msg: ReplicationMessage):
        payload = json.loads(msg.payload)
        next_lsn = payload["nextlsn"]

        changes = payload.get("change", [])
        for change in changes:
            self.__handle_change(change, next_lsn)

        # Always report success to the server to avoid a “disk full” condition.
        # https://www.psycopg.org/docs/extras.html#psycopg2.extras.ReplicationCursor.consume_stream
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

        asyncio.new_event_loop().run_until_complete(
            self.queue.put(  # type: ignore
                Event(
                    type=event_type,
                    table=table,
                    data=values,
                    progress={"start_lsn": next_lsn},
                )
            )
        )

    async def get_count(self, sync: Sync):
        with self.conn_dict.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM \"{sync.table}\"")
            ret = cur.fetchone()
            return ret[0]

    async def __aiter__(self):
        self.queue = Queue()
        await self._manage_replication_slots() 
        try:
            self.cursor.create_replication_slot(self.slot, output_plugin="wal2json")
        except psycopg2.errors.DuplicateObject:  # type: ignore
            self.slot = self._generate_unique_slot_name()
            self.cursor.create_replication_slot(self.slot, output_plugin="wal2json")

        self.cursor.start_replication(
            slot_name=self.slot,
            decode=True,
            status_interval=1,
            start_lsn=self.start_lsn,
            options={
                "include-lsn": "true",
            },
        )
        asyncio.ensure_future(
            asyncio.get_event_loop().run_in_executor(
                None, self.cursor.consume_stream, self._consumer
            )
        )
        yield ProgressEvent(
            progress={"start_lsn": self.start_lsn},
        )
        while True:
            yield await self.queue.get()
            
    async def _manage_replication_slots(self):
        """Check and manage replication slots if they exceed the max allowed."""
        def _drop_slots():
            with self.conn.cursor() as cur:
                # Retrieve all replication slots that start with 'meilisync_'
                cur.execute("SELECT slot_name, active_pid FROM pg_replication_slots WHERE slot_name LIKE 'meilisync_%'")
                meilisync_slots = cur.fetchall()

                for slot_name, active_pid in meilisync_slots:
                    if active_pid:
                        # Terminate the session
                        try:
                            cur.execute(f"SELECT pg_terminate_backend({active_pid});")
                        except psycopg2.Error as e:
                            pass

                    # Try to drop the slot after terminating the session
                    try:
                        cur.execute(f"SELECT pg_drop_replication_slot('{slot_name}');")
                    except psycopg2.errors.ObjectInUse:
                        pass
                    except psycopg2.Error as e:
                        pass
        await asyncio.get_event_loop().run_in_executor(None, _drop_slots)

        
    def _ping(self):
        with self.conn_dict.cursor() as cur:
            cur.execute("SELECT 1")

    async def ping(self):
        await asyncio.get_event_loop().run_in_executor(None, self._ping)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.conn.close()
