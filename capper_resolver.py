#!/usr/bin/env python3
"""
capper_resolver.py — Resolve raw capper names to canonical unique_capper_name values.

Replaces Claude's Stage 2 capper normalization with deterministic Python.

Resolution priority chain:
  1. Exact match against known unique_capper_name values
  2. Alias match — raw name appears in a capper's alias list
  3. Normalized key match — normalize raw name and check against keys
  4. Fuzzy match — case-insensitive substring or close match
  5. "capper_not_found" if truly unresolvable

The canonical name is always the unique_capper_name from the sheet
(lowercase with underscores), NOT the raw alias.

Usage:
    from capper_resolver import CapperResolver

    resolver = CapperResolver(spreadsheet)
    result = resolver.resolve("BEEZY WINS")
    # → "beezo_wins"
"""

import re
from typing import Dict, List, Optional

from sheets_utils import sheets_read


RESOLUTION_SHEET = "capper_name_resolution"
NOT_FOUND = "capper_not_found"


def _normalize_key(name: str) -> str:
    """Convert a raw capper name to the canonical key format.

    Rules: lowercase, a-z0-9 only, spaces become underscores.
    Same logic as populate_capper_names.normalize_capper_key().
    """
    key = name.lower().strip()
    key = key.replace(" ", "_")
    key = re.sub(r"[^a-z0-9_]", "", key)
    key = re.sub(r"_+", "_", key).strip("_")
    return key


class CapperResolver:
    """Resolve raw capper names to canonical unique_capper_name values.

    Loads capper data from capper_name_resolution sheet once, then resolves
    names via a priority chain.
    """

    def __init__(self, spreadsheet):
        """Initialize the resolver.

        Args:
            spreadsheet: gspread Spreadsheet object
        """
        print("Loading capper resolver data...")

        # canonical_names: set of all unique_capper_name values
        self.canonical_names: set = set()

        # alias_to_key: {alias_lower: unique_capper_name}
        # Maps every known alias (lowercased) to its canonical key
        self.alias_to_key: Dict[str, str] = {}

        # key_to_aliases: {unique_capper_name: [alias1, alias2, ...]}
        # For debugging/reporting
        self.key_to_aliases: Dict[str, List[str]] = {}

        self._load(spreadsheet)

    def _load(self, spreadsheet):
        """Load capper data from the resolution sheet."""
        try:
            ws = sheets_read(spreadsheet.worksheet, RESOLUTION_SHEET)
            rows = sheets_read(ws.get_all_values)
        except Exception as e:
            print(f"  Warning: could not load {RESOLUTION_SHEET}: {e}")
            return

        if len(rows) < 2:
            print(f"  Warning: {RESOLUTION_SHEET} is empty")
            return

        headers = rows[0]
        hcol = {h: i for i, h in enumerate(headers)}
        name_col = hcol.get("unique_capper_name", 0)
        alias_col = hcol.get("aliases", 1)

        for row in rows[1:]:
            while len(row) <= alias_col:
                row.append("")

            key = row[name_col].strip()
            aliases_str = row[alias_col].strip()

            if not key:
                continue

            self.canonical_names.add(key)

            # The key itself is an alias (trivially)
            self.alias_to_key[key] = key

            # Parse comma-separated aliases
            aliases = []
            if aliases_str:
                for alias in aliases_str.split(","):
                    alias = alias.strip()
                    if alias:
                        aliases.append(alias)
                        self.alias_to_key[alias.lower()] = key

            self.key_to_aliases[key] = aliases

        print(f"  {len(self.canonical_names)} cappers, "
              f"{len(self.alias_to_key)} alias keys loaded")

    def resolve(self, raw_name: str) -> str:
        """Resolve a raw capper name to the canonical unique_capper_name.

        Args:
            raw_name: Raw capper name from Stage 1 OCR (e.g. "BEEZY WINS")

        Returns:
            The canonical unique_capper_name (e.g. "beezo_wins"), or
            "capper_not_found" if no match.
        """
        raw = raw_name.strip()
        if not raw:
            return NOT_FOUND

        # ── 1. Exact match against canonical names ────────────────────────
        #    (raw name is already in key format)
        if raw in self.canonical_names:
            return raw

        # ── 2. Alias match — raw name appears in alias list ───────────────
        raw_lower = raw.lower()
        if raw_lower in self.alias_to_key:
            return self.alias_to_key[raw_lower]

        # ── 3. Normalized key match — normalize and check ─────────────────
        normalized = _normalize_key(raw)
        if normalized in self.canonical_names:
            return normalized

        # Also check if normalized form matches any alias
        if normalized in self.alias_to_key:
            return self.alias_to_key[normalized]

        # ── 4. Fuzzy match — substring / contains ─────────────────────────
        result = self._fuzzy_match(raw_lower, normalized)
        if result:
            return result

        # ── 5. Unresolvable ───────────────────────────────────────────────
        return NOT_FOUND

    def _fuzzy_match(self, raw_lower: str, normalized: str) -> Optional[str]:
        """Try fuzzy matching strategies.

        Only matches if exactly one candidate is found. Ambiguous matches
        return None to avoid false positives.
        """
        # Strategy A: normalized key is a substring of a canonical name (or vice versa)
        # e.g. "beezy" → "beezo_wins" if close enough
        candidates = set()
        for key in self.canonical_names:
            if len(normalized) >= 3 and len(key) >= 3:
                if normalized in key or key in normalized:
                    candidates.add(key)

        if len(candidates) == 1:
            return candidates.pop()

        # Strategy B: check if raw_lower matches any alias via substring
        candidates = set()
        for alias_lower, key in self.alias_to_key.items():
            if len(raw_lower) >= 4 and len(alias_lower) >= 4:
                if raw_lower in alias_lower or alias_lower in raw_lower:
                    candidates.add(key)

        if len(candidates) == 1:
            return candidates.pop()

        return None
