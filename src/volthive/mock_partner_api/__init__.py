"""A local FastAPI server standing in for the roaming partner network.

Exists so the project never depends on a live external API. It serves the same
deterministic dataset the file-mode client reads, so both paths return
identical records - which is what makes the client-equivalence test meaningful
rather than tautological.

Optional: ``docker compose --profile with-api up``. The default mode is
``file``, and CI never starts this container.
"""

from __future__ import annotations

from volthive.mock_partner_api.app import app, create_app, load_dataset

__all__ = ["app", "create_app", "load_dataset"]
