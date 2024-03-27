import typing as ty
from dataclasses import dataclass
import asyncio

import nats.aio.client as client

from nats_ez.utils import is_valid_subject

_StrOrBytes = ty.Union[str, bytes]
HandlerTypeSub = ty.Callable[[_StrOrBytes], ty.Awaitable[None]]
HandlerTypeRpc = ty.Callable[[_StrOrBytes], ty.Awaitable[_StrOrBytes]]

@dataclass
class RefInfo():
    subject: str
    queue: str
    func: ty.Callable
    is_rpc: bool

class BaseApp():
    def __init__(self, timeout: float = 5) -> None:
        self._refs = set()
        self._conn = None
        self.timeout = timeout
    
    def set_conn(self, conn: client.Client):
        if not conn.is_connected:
            raise ValueError("Provided client is not connected")
        self._conn = conn

    def register_sub(self, subject: str, func: HandlerTypeSub, queue: str = ""):
        if not is_valid_subject(subject):
            raise ValueError(f"Invalid subject: {subject}")
        self._refs.add(RefInfo(subject, queue, func, False))
    
    def register_rpc(self, subject: str, func: HandlerTypeRpc, queue: str = ""):
        if not is_valid_subject(subject):
            raise ValueError(f"Invalid subject: {subject}")
        self._refs.add(RefInfo(subject, queue, func, True))
    
    def _conn_ready(self) -> bool:
        return self._conn is not None and self._conn.is_connected

    async def _run_basic(self, ref: RefInfo):
        assert self._conn_ready()
        conn = ty.cast(client.Client, self._conn)
        sub = await conn.subscribe(ref.subject, ref.queue)
        takes_str = ty.get_type_hints(ref.func).get("msg") == str
        async for msg in sub.messages:
            data = msg.data
            if takes_str:
                data = data.decode()
            if ref.is_rpc:
                resp = await ref.func(data)
                if isinstance(resp, str):
                    resp = resp.encode()
                await conn.publish(msg.reply, resp)
            else:
                await ref.func(data)
    
    async def run(self):
        if not self._conn_ready():
            raise ValueError("Client not connected")
        tasks = [self._run_basic(ref) for ref in self._refs]
        await asyncio.gather(*tasks)
