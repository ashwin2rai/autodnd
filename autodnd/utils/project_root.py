from pathlib import Path


def get_project_root() -> Path:
    """Simple function to reliably get project root absolute path."""
    return Path(__file__).parent.parent.parent
