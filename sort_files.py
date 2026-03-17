import argparse
import asyncio
import logging
from pathlib import Path

import aiofiles
import aiofiles.os


BUFFER_SIZE = 1024 * 1024  # 1 mb
COPY_CONCURRENCY = 20


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Asynchronously sort files from source directory into output "
            "subfolders based on file extensions."
        )
    )
    parser.add_argument(
        "source",
        type=str,
        help="Path to the source folder to read files from.",
    )
    parser.add_argument(
        "output",
        type=str,
        help="Path to the output folder where sorted files will be copied.",
    )
    return parser.parse_args()


def validate_paths(source: Path, output: Path) -> None:
    if not source.exists():
        raise FileNotFoundError(f"Source folder does not exist: {source}")

    if not source.is_dir():
        raise NotADirectoryError(f"Source path is not a directory: {source}")

    if output.exists() and not output.is_dir():
        raise NotADirectoryError(f"Output path is not a directory: {output}")

    try:
        source_resolved = source.resolve()
        output_resolved = output.resolve()
        if output_resolved == source_resolved:
            raise ValueError("Source and output folders must be different.")
    except OSError:
        raise ValueError("Invalid source or output path.")


def get_extension_folder(file_path: Path) -> str:
    suffixes = file_path.suffixes

    if not suffixes:
        return "no_extension"

    clean_suffixes = [suffix.lstrip(".").lower() for suffix in suffixes if suffix]
    folder_name = "_".join(filter(None, clean_suffixes))

    return folder_name if folder_name else "no_extension"


async def ensure_dir(path: Path) -> None:
    await asyncio.to_thread(path.mkdir, parents=True, exist_ok=True)


async def get_unique_destination_path(destination: Path) -> Path:
    if not await asyncio.to_thread(destination.exists):
        return destination

    stem = destination.stem
    suffix = "".join(destination.suffixes)
    parent = destination.parent
    counter = 1

    while True:
        candidate = parent / f"{stem}_{counter}{suffix}"
        if not await asyncio.to_thread(candidate.exists):
            return candidate
        counter += 1


async def copy_file(file_path: Path, output_folder: Path, semaphore: asyncio.Semaphore) -> None:
    async with semaphore:
        try:
            extension_folder = get_extension_folder(file_path)
            target_dir = output_folder / extension_folder
            await ensure_dir(target_dir)

            destination = target_dir / file_path.name
            destination = await get_unique_destination_path(destination)

            async with aiofiles.open(file_path, "rb") as src, aiofiles.open(destination, "wb") as dst:
                while True:
                    chunk = await src.read(BUFFER_SIZE)
                    if not chunk:
                        break
                    await dst.write(chunk)

            logging.info("Copied: %s -> %s", file_path, destination)

        except Exception as error:
            logging.exception("Failed to copy file %s: %s", file_path, error)


async def read_folder(
    source_folder: Path,
    output_folder: Path,
    semaphore: asyncio.Semaphore,
) -> None:
    try:
        entries = await asyncio.to_thread(lambda: list(source_folder.iterdir()))
    except Exception as error:
        logging.exception("Failed to read folder %s: %s", source_folder, error)
        return

    tasks = []

    for entry in entries:
        try:
            if await asyncio.to_thread(entry.is_dir):
                tasks.append(read_folder(entry, output_folder, semaphore))
            elif await asyncio.to_thread(entry.is_file):
                tasks.append(copy_file(entry, output_folder, semaphore))
        except Exception as error:
            logging.exception("Failed to process entry %s: %s", entry, error)

    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def main() -> None:
    setup_logging()
    args = parse_args()

    source = Path(args.source)
    output = Path(args.output)

    try:
        validate_paths(source, output)
        await ensure_dir(output)

        semaphore = asyncio.Semaphore(COPY_CONCURRENCY)
        await read_folder(source, output, semaphore)

        logging.info("File sorting completed successfully.")

    except Exception as error:
        logging.exception("Application error: %s", error)
        raise SystemExit(1) from error


if __name__ == "__main__":
    asyncio.run(main())

