import argparse
import hashlib
from pathlib import Path

from pydantic import ValidationError

from .config import configured_store
from .store import VersionConflict


def main() -> None:
    parser = argparse.ArgumentParser(description="Import synthetic CDP-1 order snapshots.")
    parser.add_argument("files", type=Path, nargs="*", help="One synthetic order JSON per file")
    parser.add_argument("--export-archive", type=Path, help="Export committed snapshots to a new directory")
    args = parser.parse_args()
    if bool(args.files) == bool(args.export_archive):
        parser.error("provide input files or --export-archive")
    store = configured_store()
    if args.export_archive:
        try:
            export_archive(store, args.export_archive)
        except (OSError, ValueError) as error:
            parser.exit(1, f"{error}\n")
        return
    for path in args.files:
        try:
            outcome = store.ingest(path.read_bytes())
        except ValidationError as error:
            details = error.errors(include_input=False, include_context=False, include_url=False)
            fields = ", ".join(
                "unexpected field" if issue["type"] == "extra_forbidden"
                else ".".join(str(part) for part in issue["loc"]) or "order"
                for issue in details
            )
            parser.exit(1, f"order failed contract validation ({fields})\n")
        except (OSError, VersionConflict) as error:
            parser.exit(1, f"{error}\n")
        print(outcome)


def export_archive(store, directory: Path) -> None:
    store.initialize()
    directory.mkdir()
    for raw in store.archived_orders():
        digest = hashlib.sha256(raw).hexdigest()
        (directory / f"{digest}.json").write_bytes(raw)


if __name__ == "__main__":
    main()
