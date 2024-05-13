from typing import Annotated, TypeVar, Iterable, Type

from nats.aio.msg import Msg

class _Position:
    """Base position type"""

    requires_num = False
    requires_type = False

    @classmethod
    def _extract_position(cls, msg: Msg, num: int | None, target_type: Type | None):
        raise NotImplementedError


class _Body(_Position):
    """Position type for body"""
    requires_type = True

    @classmethod
    def _extract_position(cls, msg: Msg, num: int | None, target_type: Type | None):
        assert target_type is not None
        data = msg.data
        if target_type == bytes:
            return data
        elif target_type == str:
            return data.decode()
        else:
            raise ValueError(f"Unsupported target type {target_type}")

class _Subject(_Position):
    """Position type for subject"""
    requires_type = True

    @classmethod
    def _extract_position(cls, msg: Msg, num: int | None, target_type: Type | None):
        assert target_type is not None
        subj = msg.subject
        if target_type == str:
            return subj.encode()
        elif target_type == list[str]:
            return subj.split(".")
        elif target_type == tuple[str]:
            return tuple(subj.split("."))
        else:
            raise ValueError(f"Unsupported target type {target_type}")


class _SubjectToken(_Position):
    """Position type for subject portion"""
    requires_num = True

    @classmethod
    def _extract_position(cls, msg: Msg, num: int | None, target_type: Type | None):
        assert num is not None
        subj = msg.subject.split(".")
        return subj[num]

class _SubjectTokenSequence(_Position):
    """Position type for subject sequence"""
    requires_num = True
    requires_type = True

    @classmethod
    def _extract_position(cls, msg: Msg, num: int | None, target_type: Type | None):
        assert num is not None
        assert target_type is not None
        subj = msg.subject.split(".")
        if target_type == str:
            return ".".join(subj[num:])
        elif target_type == list[str]:
            return subj[num:]
        elif target_type == tuple[str]:
            return tuple(subj[num:])

class _Headers(_Position):
    """Position type for headers"""

    @classmethod
    def _extract_position(cls, msg: Msg, num: int | None, target_type: Type | None):
        headers = msg.headers or {}
        return headers

class _Message(_Position):
    """Position type for whole message"""

    @classmethod
    def _extract_position(cls, msg: Msg, num: int | None, target_type: Type | None):
        return msg

_s_b = str | bytes

B = TypeVar("B", bound=_s_b)
S = TypeVar("S", bound=str | list[str] | tuple[bytes])


Body = Annotated[B, _Body()]
'''Generic type for message body'''

Subject = Annotated[S, _Subject()]
'''Generic type for whole subject'''

Headers = Annotated[dict[str, str], _Headers()]
'''Type for all message headers'''

SubjectToken = Annotated[str, _SubjectToken()]
'''Type for single subject token, marked as '*' wildcard'''

SubjectTokenSequence = Annotated[S, _SubjectTokenSequence()]
'''Generic type for sequence of subject tokens, marked as '>' wildcard'''

Message = Annotated[Msg, _Message()]
'''Type for whole message object'''

SUPPORTED_TYPES = [Body, Subject, Headers, SubjectToken, SubjectTokenSequence, Message]

def extract_position(msg: Msg, num: int | None, elem_type: Type):
    if elem_type not in SUPPORTED_TYPES:
        raise ValueError(f"Unsupported target type {elem_type}")
    position = elem_type.__metadata__[0]
    target_type = elem_type.__origin__
    return position._extract_position(msg, num, target_type)

__all__ = ["Body", "Subject", "Headers", "SubjectToken", "SubjectTokenSequence", "Message", "SUPPORTED_TYPES"]