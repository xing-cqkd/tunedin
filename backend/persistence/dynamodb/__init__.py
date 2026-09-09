"""DynamoDB persistence backend for tunedin (Linear: XIN-89).

Phase 1 scaffolding: key builders (:mod:`keys`) and table provisioning
(:mod:`table`).  The ``DynamoDBStore`` repository implementations land in
XIN-90.
"""

from . import keys, table

__all__ = ["keys", "table"]
