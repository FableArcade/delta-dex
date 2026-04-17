"""Narrow-target models.

The generic 90-day return model is thin-edge. Narrow targets condition on
discrete events (reprint announcement, PSA pop bump, etc.) where the
signal-to-noise ratio is higher.

Each target exposes:
  detect(db, as_of) -> list[Event]
  featurize(db, event) -> dict
  predict(db, event) -> predicted_return
  run(db, as_of) -> writes rows to narrow_target_predictions

Until we have labeled historical events to fit on, the `predict` step
returns None / a heuristic. The scaffolding is runnable end-to-end so the
pipeline agent can wire it into the daily job today.
"""

from . import reprint_event, pop_bump

__all__ = ["reprint_event", "pop_bump"]
