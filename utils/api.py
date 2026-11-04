from typing import Any


def validate_similarity_cutoff(value: float | None) -> float | None:
    """Accept None (use the configured default) or a cutoff between 0.0 and 1.0."""
    if value is not None and not (0.0 <= value <= 1.0):
        raise ValueError("similarity_cutoff must be between 0.0 and 1.0")
    return value


def format_chunks(nodes_with_score: list[Any]) -> list[str]:
    """Format retrieved nodes as human-readable strings with score and text."""
    chunks: list[str] = []

    for node_with_score in nodes_with_score:
        score = node_with_score.score

        if score is None:
            chunks.append(f"Score: n/a | Text: {node_with_score.node.get_text()}")
            continue

        chunks.append(f"Score: {score:.4f} | Text: {node_with_score.node.get_text()}")

    return chunks
