"""Control de tasa y rampa de carga hacia un Sink (RNF-01: hasta 5.000 ev/s)."""
from __future__ import annotations

import time
from dataclasses import dataclass, asdict

from .events import EventFactory
from .sinks import Sink


@dataclass
class StepResult:
    target_rate: int
    seconds: float
    emitted: int
    achieved_rate: float
    sustained: bool  # alcanzó >= 95 % de la tasa objetivo


def run_step(factory: EventFactory, sink: Sink, rate: int, seconds: float, tick: float = 0.1,
             clock=time.perf_counter, sleep=time.sleep) -> StepResult:
    """Emite a `rate` ev/s durante `seconds`. Si el generador o el sink no alcanzan, lo reporta
    en `sustained` en vez de acumular deuda (no hay ráfagas de recuperación mayores a 1 s)."""
    start = clock()
    emitted = 0
    next_tick = start
    while True:
        now = clock()
        elapsed = now - start
        if elapsed >= seconds:
            break
        due = int(rate * elapsed) - emitted
        due = min(due, rate)  # tope de recuperación: 1 s de eventos
        if due > 0:
            batch = factory.next_batch(due, now_s=elapsed)
            sink.send(batch)
            emitted += len(batch)
        next_tick += tick
        delay = next_tick - clock()
        if delay > 0:
            sleep(delay)
    total = clock() - start
    achieved = emitted / total if total else 0.0
    return StepResult(rate, round(total, 2), emitted, round(achieved, 1), achieved >= 0.95 * rate)


def run_ramp(factory: EventFactory, sink: Sink, rates: list[int], step_seconds: float, on_step=None) -> list[StepResult]:
    results = []
    for r in rates:
        res = run_step(factory, sink, r, step_seconds)
        results.append(res)
        if on_step:
            on_step(res)
    return results


def results_to_dicts(results: list[StepResult]) -> list[dict]:
    return [asdict(r) for r in results]
