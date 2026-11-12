"""What every component gets: config, logger, control API client, producer, stop signal."""

from __future__ import annotations

import logging
import signal
import threading
from collections.abc import Sequence
from typing import Any

from leasehold.client import ControlClient, NewTask
from leasehold.runtime import log as lh_log
from leasehold.runtime import resources
from leasehold.runtime.config import ConfigError, RuntimeConfig
from leasehold.runtime.events import Delivery, EventProducer
from leasehold.runtime.http import Liveness, serve


class Context:
    def __init__(
        self,
        cfg: RuntimeConfig,
        client: ControlClient | None = None,
        producer: EventProducer | None = None,
        logger: logging.Logger | None = None,
    ):
        self.cfg = cfg
        self.client = client
        self.producer = producer
        self.log = logger or logging.getLogger("leasehold")
        self.stopping = threading.Event()
        self.liveness = Liveness()
        self._http = None

    # --- for component code -------------------------------------------------------------------

    def emit(self, topic: str, payload: Any, **kw) -> Delivery:
        if self.producer is None:
            raise ConfigError(f"{self.cfg.name} produces no topics")
        return self.producer.emit(topic, payload, **kw)

    def upsert(self, task_type: str, tasks: Sequence[NewTask], reopen: bool = False):
        """Create tasks of one of this service's task types (in ``creates_tasks``)."""
        if task_type not in self.cfg.component_spec.creates_tasks:
            raise ConfigError(
                f"{self.cfg.name} may not create {task_type}; add it to creates_tasks"
            )
        if self.client is None:
            raise ConfigError("no control API client")
        return self.client.upsert(task_type, tasks, reopen=reopen)

    # --- lifecycle ----------------------------------------------------------------------------

    def install_signal_handlers(self) -> None:
        def stop(signum, _frame):
            if not self.stopping.is_set():
                self.log.info("stopping", extra={"fields": {"signal": signal.Signals(signum).name}})
            self.stopping.set()

        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)

    def serve_http(self) -> None:
        resources.register(self.cfg.version)
        if self.cfg.http_port:
            self._http = serve(self.cfg.http_port, self.liveness)
            self.log.info(
                "serving /health and /metrics",
                extra={"fields": {"port": self._http.server_address[1]}},
            )

    def close(self) -> None:
        if self.producer is not None:
            self.producer.close()
        if self._http is not None:
            self._http.shutdown()
        if self.client is not None:
            self.client.close()


def build(
    kind: str,
    cfg: RuntimeConfig | None = None,
    client: ControlClient | None = None,
    producer: EventProducer | None = None,
) -> Context:
    """Set up a component of ``kind`` from the environment (or the given parts)."""
    cfg = cfg or RuntimeConfig.from_env()
    comp = cfg.component_spec
    if comp.kind != kind:
        raise ConfigError(f"{cfg.name} is a {comp.kind}, not a {kind}")
    logger = lh_log.setup(cfg.service, cfg.component, cfg.env, cfg.version, cfg.instance)
    if client is None and (kind == "task-worker" or comp.creates_tasks):
        client = ControlClient.from_env()
    if producer is None and comp.produces:
        producer = EventProducer(cfg)
    return Context(cfg, client=client, producer=producer, logger=logger)
