"""Helpers for exporting post metadata JSON files."""


import json

from pathlib import Path
from typing import Any

from config import FanslyConfig
from pathio import set_create_directory_for_download

from .downloadstate import DownloadState


def collect_posts_records(items: list[dict[str, Any]], source: str) -> list[dict[str, Any]]:
    """Normalize raw post/message entries into a stable JSON structure."""
    records: list[dict[str, Any]] = []

    for item in items:
        if not isinstance(item, dict):
            continue

        account_media_ids = item.get('accountMediaIds')

        if not isinstance(account_media_ids, list):
            account_media_ids = []

        text = item.get('text')

        if text is None:
            text = item.get('content')

        records.append(
            {
                'id': str(item.get('id', '')),
                'source': source,
                'created_at': item.get('createdAt'),
                'updated_at': item.get('updatedAt'),
                'text': text,
                'account_media_ids': account_media_ids,
                'raw': item,
            }
        )

    return records


def _read_existing_json(json_path: Path) -> list[dict[str, Any]]:
    if not json_path.exists():
        return []

    try:
        loaded = json.loads(json_path.read_text(encoding='utf-8'))

        if isinstance(loaded, list):
            return loaded

    except Exception:
        pass

    return []


def save_posts_json(
            config: FanslyConfig,
            state: DownloadState,
            filename: str,
            records: list[dict[str, Any]],
        ) -> Path | None:
    """Save unique post records in a JSON file in the current mode folder."""
    if len(records) == 0:
        return None

    base_directory = set_create_directory_for_download(config, state)
    json_path = base_directory / filename

    existing = _read_existing_json(json_path)
    existing_ids = {
        str(entry.get('id', ''))
        for entry in existing
        if isinstance(entry, dict)
    }

    for record in records:
        if not isinstance(record, dict):
            continue

        record_id = str(record.get('id', ''))

        if record_id and record_id in existing_ids:
            continue

        existing.append(record)

        if record_id:
            existing_ids.add(record_id)

    json_path.write_text(
        json.dumps(existing, indent=2, ensure_ascii=False),
        encoding='utf-8',
    )

    return json_path