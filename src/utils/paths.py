"""Filesystem helpers so jobs never hardcode absolute paths."""
from __future__ import annotations

import shutil
from pathlib import Path


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def as_posix(path: Path) -> str:
    """Spark wants forward-slash paths even on platforms that use '\\'."""
    return path.resolve().as_posix()


def write_single_csv(df, target_path: Path) -> Path:
    """Writes a DataFrame as one flat, portfolio-friendly CSV file instead of
    Spark's default part-file-per-partition directory."""
    ensure_dir(target_path.parent)
    tmp_dir = target_path.parent / f".tmp_{target_path.stem}"
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)

    df.coalesce(1).write.mode("overwrite").option("header", True).csv(as_posix(tmp_dir))

    part_file = next(tmp_dir.glob("part-*.csv"))
    if target_path.exists():
        target_path.unlink()
    shutil.move(str(part_file), str(target_path))
    shutil.rmtree(tmp_dir)
    return target_path
