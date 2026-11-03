"""Integrations: TradingView webhook, Telegram/Discord notifications, Prometheus, OpenTelemetry.

All are off by default and the core does not import them. Third-party packages are extras:
``prometheus`` (prometheus_client), ``otel`` (opentelemetry-api), ``fastapi`` (webhook router).
Notifications and the aiohttp webhook server run on the core's aiohttp.

- :mod:`bytx.integrations.notify` — :class:`Notifier`, :class:`TelegramChannel`, :class:`DiscordChannel`;
- :mod:`bytx.integrations.tradingview` — :class:`TradingViewWebhook`, ``aiohttp_app``, ``fastapi_router``;
- :mod:`bytx.integrations.prometheus` — :class:`PrometheusCollector`;
- :mod:`bytx.integrations.otel` — :func:`instrument` (REST / WS / order spans).
"""
