from __future__ import annotations

from typing import Any, Mapping


class Item(dict[str, Any]):
    """A tiny Scrapy-like item.

    Spider callbacks may yield ``Item`` or a plain mapping; both are normalized
    before they reach pipelines.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "Item":
        return cls(value)
