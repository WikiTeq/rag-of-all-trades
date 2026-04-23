from __future__ import annotations


def mutually_exclusive(cfg: dict, key_a: str, key_b: str, source: str) -> None:
    """Raise ValueError if both key_a and key_b are truthy in cfg."""
    if cfg.get(key_a) and cfg.get(key_b):
        raise ValueError(f"'{key_a}' and '{key_b}' are mutually exclusive in {source} config")


def require_one_of(cfg: dict, keys: list[str] | tuple[str, ...], source: str) -> str:
    """Raise ValueError unless exactly one key in keys is truthy in cfg.

    Returns the active key name.
    """
    active = [k for k in keys if cfg.get(k)]
    if len(active) != 1:
        raise ValueError(f"Exactly one of {list(keys)} must be set in {source} config; got {active or 'none'}")
    return active[0]
