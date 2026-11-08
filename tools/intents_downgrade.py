"""Roll back an intent store (IntentStore, sqlite) to the schema of an older SDK.

An older SDK refuses to open a database with a schema newer than it knows (``ConfigError``) — by design: silently
reading an unknown schema is more dangerous. Rolling back the SDK is one of two:

1. a new (empty) database: the old process starts with another file, ``recover()`` over the previous intents is not
   needed;
2. this tool — with the process STOPPED (a process holding the ``<db>.lock`` lock — refused)::

       python tools/intents_downgrade.py bot.sqlite --to 3

   - 5 → 4: namespaces without the key fingerprint (``exchange:market:mode:account``, as in schemas 1–4); the
     ``intents.plan_retired`` column stays (schema 4 does not read it), meta.schema = 4. Closed entry plans
     (``plan_retired``) are open again for the older version — close them yourself before the rollback or accept that
     recover() of the older version may place their legs;
   - → 3 (bytx 0.1.5 and earlier): also meta.schema = 3 (version 3 does not read the ``algos.plan`` column).

   A client_id conflict when stripping fingerprints (one database served several keys) — refused without changes
   (``--force`` — keep the conflicting rows with the fingerprint: the older version will not see them).

On upgrading again the database migrates anew (the columns are already there); rows without the fingerprint — see
``IntentStore.claim_legacy``.
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from typing import Optional


def _legacy(namespace: str) -> Optional[str]:
    """``exchange:market:mode:account:fingerprint`` → ``exchange:market:mode:account``; already without — None."""
    parts = namespace.split(':')
    return ':'.join(parts[:4]) if len(parts) >= 5 else None


def downgrade(path: str, to: int, *, force: bool = False) -> dict:
    """Roll the database ``path`` back to schema ``to`` (3 or 4). Returns {'renamed': n, 'kept': n, 'schema': to}."""
    if to not in (3, 4):
        raise SystemExit('--to: 3 or 4')
    from bytx.recovery.store import _acquire_lock, _release_lock
    key = _acquire_lock(path)          # an SDK process holds the database — ConfigError
    try:
        db = sqlite3.connect(path, isolation_level=None)
        try:
            current = int(db.execute("SELECT value FROM meta WHERE key='schema'").fetchone()[0])
            if current <= to:
                return {'renamed': 0, 'kept': 0, 'schema': current}
            db.execute('BEGIN IMMEDIATE')
            renamed = kept = 0
            for table, pk in (('intents', 'client_id'), ('algos', 'algo_id')):
                rows = db.execute(f'SELECT rowid, namespace, {pk} FROM {table}').fetchall()
                taken = {(ns, k) for _, ns, k in rows if _legacy(ns) is None}
                for rowid, ns, k in rows:
                    old = _legacy(ns)
                    if old is None:
                        continue
                    if (old, k) in taken:
                        if not force:
                            db.execute('ROLLBACK')
                            raise SystemExit(f'{table}: {k} exists both in {old!r} and in {ns!r} — rollback without '
                                             f'changes (--force — keep the row with the fingerprint)')
                        kept += 1
                        continue
                    db.execute(f'UPDATE {table} SET namespace=? WHERE rowid=?', (old, rowid))
                    taken.add((old, k))
                    renamed += 1
            db.execute("DELETE FROM meta WHERE key LIKE 'claimed:%'")
            db.execute("UPDATE meta SET value=? WHERE key='schema'", (str(to),))
            db.execute('COMMIT')
            return {'renamed': renamed, 'kept': kept, 'schema': to}
        finally:
            db.close()
    finally:
        _release_lock(key)


def main(argv: Optional[list] = None) -> None:
    parser = argparse.ArgumentParser(description=(__doc__ or '').splitlines()[0])
    parser.add_argument('path')
    parser.add_argument('--to', type=int, required=True, choices=(3, 4))
    parser.add_argument('--force', action='store_true')
    args = parser.parse_args(argv)
    print(downgrade(args.path, args.to, force=args.force))


if __name__ == '__main__':
    main(sys.argv[1:])
