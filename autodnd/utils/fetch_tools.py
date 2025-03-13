from pathlib import Path
import requests


def download_file(target_dir: Path, filename: str, url: str) -> None:
    """Function to download and save a file from an url."""
    # Ensure target directory (and parents) exist
    target_dir.mkdir(parents=True, exist_ok=True)
    target_file = target_dir / filename
    # Download the file
    print(f"Downloading from {url} ...")
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()  # Raise an error if the request failed
        with open(target_file, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

    print(f"File saved to {target_file}")
