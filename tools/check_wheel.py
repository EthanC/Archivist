"""Validate the contents and metadata of a built wheel."""

from __future__ import annotations

from email.parser import BytesParser
from pathlib import Path, PurePosixPath
from zipfile import ZipFile


def main() -> None:
    """Check the single wheel in the distribution directory."""
    wheels = tuple(Path("dist").glob("archivist_py-*.whl"))
    if len(wheels) != 1:
        raise RuntimeError(f"expected one wheel, found {len(wheels)}")

    with ZipFile(wheels[0]) as archive:
        names = frozenset(archive.namelist())
        required_package_files = {
            "archivist/__init__.py",
            "archivist/py.typed",
        }
        missing = required_package_files - names
        if missing:
            raise RuntimeError(f"wheel is missing package files: {sorted(missing)}")

        metadata_name = next(
            (name for name in names if name.endswith(".dist-info/METADATA")), None
        )
        if metadata_name is None:
            raise RuntimeError("wheel has no METADATA file")
        metadata = BytesParser().parsebytes(archive.read(metadata_name))
        if metadata["Name"] != "archivist-py":
            raise RuntimeError("wheel has the wrong distribution name")
        if metadata["License-Expression"] != "AGPL-3.0-only":
            raise RuntimeError("wheel has the wrong license expression")
        if "# Archivist" not in metadata.get_payload():
            raise RuntimeError("wheel metadata does not contain the README")

        has_license = any(
            PurePosixPath(name).name == "LICENSE" and ".dist-info" in name
            for name in names
        )
        if not has_license:
            raise RuntimeError("wheel does not contain LICENSE")


if __name__ == "__main__":
    main()
