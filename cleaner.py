#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Простой скрипт: при запуске удаляет пустые папки (рекурсивно) в D:\\lorett\\data\\decoded.
"""

import os
import sys
from pathlib import Path


def remove_empty_dirs(root: Path, *, dry_run: bool = True, remove_root: bool = False) -> tuple[int, int]:
    """
    Удаляет пустые директории внутри root (и, опционально, сам root если он пуст).

    Возвращает (кол-во удалённых папок, кол-во ошибок).
    """
    removed = 0
    errors = 0

    if not root.exists():
        raise FileNotFoundError(f"Path does not exist: {root}")
    if not root.is_dir():
        raise NotADirectoryError(f"Path is not a directory: {root}")

    # bottom-up: сначала вложенные, затем родительские
    for dirpath, dirnames, filenames in os.walk(root, topdown=False):
        p = Path(dirpath)

        # Быстрое условие пустоты: нет файлов и нет подпапок (после возможных удалений ниже).
        # Но чтобы учесть гонки/права, дополнительно проверяем listdir.
        try:
            if not any(p.iterdir()):
                if p == root and not remove_root:
                    continue
                if dry_run:
                    print(f"[DRY] rmdir {p}")
                else:
                    p.rmdir()
                    print(f"[OK ] rmdir {p}")
                removed += 1
        except Exception as e:
            errors += 1
            print(f"[ERR] {p}: {e}", file=sys.stderr)

    return removed, errors


ROOT = Path(r"D:\lorett\data\decoded")


def main() -> int:
    removed, errors = remove_empty_dirs(ROOT, dry_run=False, remove_root=False)
    print(f"[DELETE] root={ROOT} removed_dirs={removed} errors={errors}")
    return 2 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
