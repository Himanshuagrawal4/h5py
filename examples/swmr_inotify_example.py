"""Monitor an append-only SWMR dataset using watchdog filesystem events."""

import argparse
import logging
from pathlib import Path
from threading import Event

import h5py
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer


class FileChanges(FileSystemEventHandler):
    def __init__(self, filename, changed):
        self.filename = filename
        self.changed = changed

    def on_modified(self, event):
        if not event.is_directory and Path(event.src_path).resolve() == self.filename:
            self.changed.set()


def monitor_dataset(filename, dataset_name):
    filename = Path(filename).resolve()
    changed = Event()
    observer = Observer()
    observer.schedule(FileChanges(filename, changed), str(filename.parent))

    with h5py.File(filename, "r", libver="latest", swmr=True) as file:
        dataset = file[dataset_name]
        last_size = 0
        observer.start()
        changed.set()
        try:
            while observer.is_alive():
                if not changed.wait(timeout=1):
                    continue
                changed.clear()
                dataset.refresh()
                size = dataset.shape[0]
                if size > last_size:
                    logging.info(
                        "Read rows %d:%d: %s", last_size, size, dataset[last_size:size]
                    )
                    last_size = size
        except KeyboardInterrupt:
            pass
        finally:
            observer.stop()
            observer.join()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("filename", nargs="?", default="swmr.h5")
    parser.add_argument("dataset", nargs="?", default="data")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    monitor_dataset(args.filename, args.dataset)
