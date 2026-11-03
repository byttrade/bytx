"""Position journal: models, store, next-generation client_id, collector.

::

    from bytx.journal import PositionJournal

    journal = PositionJournal(bot_id=bot_id)          # in-memory; a service brings its own JournalStore
    ex = create_exchange('okx', ..., journal=journal)
    with journal.bind(position_id=cycle_id, role='entry'):
        await ex.buy('BTC/USDT', margin=50, leverage=5)
    events = await journal.read(cycle_id)

    # recovery from exchange history and interventions
    res = await journal.recover(ex, 'BTC/USDT', 'long', since=opened_ms)
"""
from bytx.journal.client_id import (
    CODE_ROLES,
    LEVEL_ROLES,
    ROLE_CODES,
    ClientIdInfo,
    bot_tag,
    make_client_id,
    parse_client_id,
)
from bytx.journal.journal import Binding, PositionJournal, current_binding, own_size
from bytx.journal.models import (
    EVENTS,
    INTERVENTION_KINDS,
    JOURNAL_SCHEMA_VERSION,
    ORIGINS,
    OUTCOMES,
    ROLES,
    JournalEntry,
    JournalEvent,
    OrderSummary,
    PositionRecord,
)
from bytx.journal.origin import OriginVerdict, classify_origin, rules_for
from bytx.journal.recovery import ExchangeHistorySource, HistorySource, RecoveryResult, recover_position
from bytx.journal.store import AppendResult, InMemoryJournalStore, JournalStore

__all__ = [
    'AppendResult', 'Binding', 'CODE_ROLES', 'ClientIdInfo', 'EVENTS', 'ExchangeHistorySource', 'HistorySource',
    'INTERVENTION_KINDS', 'InMemoryJournalStore', 'JOURNAL_SCHEMA_VERSION', 'JournalEntry', 'JournalEvent',
    'JournalStore', 'LEVEL_ROLES', 'ORIGINS', 'OUTCOMES', 'OrderSummary', 'OriginVerdict', 'PositionJournal',
    'PositionRecord', 'ROLES', 'ROLE_CODES', 'RecoveryResult', 'bot_tag', 'classify_origin',
    'current_binding', 'make_client_id', 'own_size', 'parse_client_id', 'recover_position', 'rules_for',
]
