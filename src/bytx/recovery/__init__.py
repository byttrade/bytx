"""Recovery after restart: intent store and reconciliation with the exchange.

::

    from bytx.recovery import IntentStore

    ex = create_exchange('okx', ..., client_id_prefix='bot42', intents=IntentStore('bot42.sqlite'))
    report = await ex.recover(resubmit=False)      # at process start, before new orders
    for order in report.live:                       # live orders — keep managing them
        ...

Every order is written to sqlite BEFORE sending (``pending``), then its outcome (``placed`` /
``final`` / ``rejected`` / ``unknown``). At start :func:`recover` reconciles open intents with
the exchange by client_id: picks up live ones, marks finished ones, re-creates missing ones with the same
client_id (if allowed) or closes them as ``abandoned`` — no duplicates and no losses
(failure-point test matrix — ``tests/recovery``). The module is not imported until a store
is attached: the order hot path is unchanged without it.
"""
from bytx.recovery.reconcile import RecoveryReport, ResubmitPolicy, recover
from bytx.recovery.store import OPEN_STATES, SCHEMA_VERSION, STATES, Intent, IntentStore, namespace_of

__all__ = ['Intent', 'IntentStore', 'OPEN_STATES', 'RecoveryReport', 'ResubmitPolicy', 'SCHEMA_VERSION', 'STATES',
           'namespace_of', 'recover']
