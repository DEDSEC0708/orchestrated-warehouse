"""Source extractors, the watermark window logic and the file registry.

Ingestion MOVES BYTES AND DOES NOT INTERPRET THEM. It connects, pages, applies
the watermark predicate, counts rows, stamps lineage and bulk-loads into
``raw``. It explicitly does not cast business types, filter "bad" rows, derive
columns or join.

That restraint is the most important architectural decision in the project. If
ingestion decides what is valid, the evidence of what actually arrived is
destroyed - and a transformation bug becomes unfixable without re-reading a
source that may no longer hold the data. Everything downstream can be rebuilt
from ``raw``; ``raw`` can only be rebuilt from a source system.
"""

from __future__ import annotations

from volthive.ingest.cms import CMS_ENTITIES, ingest_cms_entity
from volthive.ingest.files import ingest_file_source, ingest_seed_file
from volthive.ingest.lineage import payload_hash, row_hash
from volthive.ingest.partner_api import (
    FilePartnerCdrClient,
    HttpPartnerCdrClient,
    PartnerCdrClient,
    build_partner_client,
    ingest_partner_cdrs,
)
from volthive.ingest.registry import (
    DiscoveredFile,
    discover_partition_files,
    file_sha256,
    is_already_ingested,
    register_file,
)
from volthive.ingest.watermark import (
    ExtractWindow,
    Watermark,
    advance_watermark,
    compute_window,
    read_watermark,
)

__all__ = [
    "CMS_ENTITIES",
    "DiscoveredFile",
    "ExtractWindow",
    "FilePartnerCdrClient",
    "HttpPartnerCdrClient",
    "PartnerCdrClient",
    "Watermark",
    "advance_watermark",
    "build_partner_client",
    "compute_window",
    "discover_partition_files",
    "file_sha256",
    "ingest_cms_entity",
    "ingest_file_source",
    "ingest_partner_cdrs",
    "ingest_seed_file",
    "is_already_ingested",
    "payload_hash",
    "read_watermark",
    "register_file",
    "row_hash",
]
