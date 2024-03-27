import re

_SUBJECT_RE = re.compile(r'^(?:(?:\*|[a-zA-Z0-9_-]*)\.)*(?:\*|>|[a-zA-Z0-9_-]*)$')

def is_valid_subject(subject: str) -> bool:
    """
    Check if the string is a valid NATS subject.
    """
    return _SUBJECT_RE.match(subject) is not None
    