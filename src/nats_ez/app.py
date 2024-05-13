import typing as ty
from dataclasses import dataclass
import asyncio

import nats.aio.client as client
from nats.aio.msg import Msg

from nats_ez.types import SUPPORTED_TYPES, extract_position

HandlerTypeSub = ty.Callable[..., ty.Awaitable[None]]
HandlerTypeRpc = ty.Callable[..., ty.Awaitable[bytes | str]]


@dataclass
class _RefInfo:
    subject: str
    queue: str
    func: ty.Callable
    is_rpc: bool


class BaseApp:
    def __init__(self, timeout: float = 5) -> None:
        self._refs = set()
        self._conn = None
        self.timeout = timeout

    def set_conn(self, conn: client.Client):
        if not conn.is_connected:
            raise ValueError("Provided client is not connected")
        self._conn = conn

    def register_sub(self, subject: str, func: HandlerTypeSub, queue: str = ""):
        self._refs.add(_RefInfo(subject, queue, func, False))

    def register_rpc(self, subject: str, func: HandlerTypeRpc, queue: str = ""):
        self._refs.add(_RefInfo(subject, queue, func, True))

    def _conn_ready(self) -> bool:
        return self._conn is not None and self._conn.is_connected

    async def _apply_callback_function(self, ref: _RefInfo, msg: Msg) -> bytes | None:
        type_hints = ty.get_type_hints(ref.func, include_extras=True)
        apply_fun_dct = {}
        return_type = type_hints.pop("return")
        for arg_name, type_hint in type_hints.items():
            if type_hint in SUPPORTED_TYPES:
                apply_fun_dct[arg_name] = extract_position(msg, None, type_hint)
            else:
                raise ValueError(f"Unsupported type {type_hint}")
        result = await ref.func(**apply_fun_dct)
        if result is None:
            return None
        else:
            assert isinstance(result, return_type)
            if return_type == str:
                return result.encode()
            else:
                return result

    async def _run_basic(self, ref: _RefInfo):
        assert self._conn_ready()
        conn = ty.cast(client.Client, self._conn)
        sub = await conn.subscribe(ref.subject, ref.queue)
        takes_str = ty.get_type_hints(ref.func).get("msg") == str
        async for msg in sub.messages:
            data = msg.data
            if takes_str:
                data = data.decode()
            resp = await self._apply_callback_function(ref, msg)
            if ref.is_rpc:
                if resp is None:
                    await conn.publish(msg.reply, b"")
                else:
                    await conn.publish(msg.reply, resp)

    async def run(self):
        if not self._conn_ready():
            raise ValueError("Client not connected")
        tasks = [self._run_basic(ref) for ref in self._refs]
        await asyncio.gather(*tasks)
