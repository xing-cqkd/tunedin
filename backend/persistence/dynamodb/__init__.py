"""DynamoDB persistence backend for tunedin (Linear: XIN-89, XIN-90).

Single-table design: key builders (:mod:`keys`), table provisioning
(:mod:`table`), model<->item codec (:mod:`codec`), the
:mod:`repositories` implementations and :class:`store.DynamoDBStore`.
:mod:`testing` is test-only support (moto-compatible async client
adapter); production code must never import it.
"""

from . import keys, table
from .store import DynamoDBStore

__all__ = ["DynamoDBStore", "keys", "table"]
