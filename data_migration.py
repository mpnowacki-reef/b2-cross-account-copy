import argparse
import concurrent.futures
import hashlib
import io
import os
import logging
import math
from contextlib import contextmanager
from typing import Tuple, Callable, Dict

from b2sdk.v1 import *
from b2sdk.v1.exception import TruncatedOutput
from b2_cross_account_copy import download_manager

try:
    import psutil
except ModuleNotFoundError:
    psutil = None

logger = logging.getLogger(__name__)


def get_api_and_bucket(key_id: str, key_env_var_name: str, bucket_name: str, sqlite_file_name: str):
    account_info = SqliteAccountInfo(sqlite_file_name)
    api = download_manager.SimpleDownloaderB2Api(account_info)
    if not account_info.is_same_key(key_id, 'production'):
        if key_env_var_name not in os.environ:
            raise ValueError(f'"{key_env_var_name}" env var is necessary for authentication')
        api.authorize_account('production', key_id, os.environ[key_env_var_name])
    bucket = api.get_bucket_by_name(bucket_name)
    return api, bucket


def main():
    args = parse_args()
    source_api, source_bucket = get_api_and_bucket(
        args.source_key_id, 'SOURCE_KEY', args.source_bucket_name, '~/.data_migrator_b2_source')
    destination_api, destination_bucket = get_api_and_bucket(
        args.destiantion_key_id, 'DESTIANTION_KEY', args.destiantion_bucket_name, '~/.data_migrator_b2_destination')
    source_file_version = source_bucket.get_file_info_by_name(args.source_file_name)
    # TODO: check for action?
    progress_listener = make_progress_listener('Total bytes transfered (download+upload)', False)
    progress_listener.set_total_bytes(int(source_file_version.size) * 2)
    started_file = destination_bucket.start_large_file(
        args.destination_file_name, source_file_version.content_type, source_file_version.file_info)

    single_thread_batch_length = math.ceil(int(source_file_version.size) / args.thread_count)
    parts_per_thread = math.ceil(single_thread_batch_length / (args.chunk_size_mb * 1024 * 1024))
    part_shas: Dict[int, str] = {}
    future_to_thread_number = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.thread_count) as executor:
        for thread_number in range(args.thread_count):
            future_to_thread_number[executor.submit(
                copy_batch,
                thread_number,
                part_shas,
                source_api,
                destination_bucket,
                started_file.file_id,
                source_file_version.id_,
                (thread_number * single_thread_batch_length,
                 min((thread_number + 1) * single_thread_batch_length, int(source_file_version.size)) - 1),
                args.chunk_size_mb * 1024 * 1024,
                parts_per_thread * thread_number,
                progress_listener,
                args.print_memory_usage,
            )] = thread_number
    for future in concurrent.futures.as_completed(future_to_thread_number):
        try:
            future.result()
        except Exception as exc:
            raise Exception(f'Thread {future_to_thread_number[future]} failed with the exception above ') from exc
        else:
            logger.info(f'Thread {future_to_thread_number[future]} finished successfully')
    sha_list = []
    for _, content_sha in sorted(part_shas.items(), key=lambda kv: kv[0]):
        sha_list.append(content_sha)
    destination_bucket.api.session.finish_large_file(started_file.file_id, sha_list)


def parse_args():
    parser = argparse.ArgumentParser(description='Copy a file between two buckets, regardless of their ownership')
    parser.add_argument('--source_key_id', type=str, required=True)
    parser.add_argument('--source_bucket_name', type=str, required=True)
    parser.add_argument('--source_file_name', type=str, required=True)

    parser.add_argument('--destiantion_key_id', type=str, required=True)
    parser.add_argument('--destiantion_bucket_name', type=str, required=True)
    parser.add_argument('--destination_file_name', type=str, required=True)

    parser.add_argument('--print_memory_usage', action='store_true', default=False)
    log_level = parser.add_mutually_exclusive_group()
    log_level.add_argument('--debug', action='store_true', default=False)
    log_level.add_argument('--info', action='store_true', default=False)

    parser.add_argument('--thread_count', type=int, default=10)
    parser.add_argument('--chunk_size_mb', type=int, default=200)

    args = parser.parse_args()
    if args.info:
        logging.basicConfig(level=logging.INFO)
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)

    if args.print_memory_usage:
        if psutil is None:
            raise ModuleNotFoundError('psutil module is required for printing memory usage')

    return args


class ThrottledBytesIO:
    def __init__(
            self,
            chunk_size: int,
            chunk_done_callback: Callable[[io.BytesIO, int, str], None],
            progress_listener: AbstractProgressListener,
            total_bytes_to_read: int,
    ):
        self.absolute_minimum_part_size = 5 * 1024 * 1024  # get it from account info
        self.current_stream = io.BytesIO()
        self.current_size = 0
        self.current_hash = hashlib.sha1()

        self.chunk_size = chunk_size
        self.chunk_done_callback = chunk_done_callback
        self.progress_listener = progress_listener

        self.left_to_read = total_bytes_to_read

    def write(self, buffer):
        total_size = len(buffer)
        left_to_write = total_size
        last_write_end = 0
        while left_to_write > 0:
            if left_to_write >= self.chunk_size - self.current_size:
                this_write_end = last_write_end + (self.chunk_size - self.current_size)
            else:
                this_write_end = last_write_end + left_to_write

            self.current_stream.write(buffer[last_write_end: this_write_end])
            self.progress_listener.bytes_completed(
                self.progress_listener.prev_value + this_write_end - last_write_end)
            self.current_hash.update(buffer[last_write_end: this_write_end])
            self.current_size += this_write_end - last_write_end
            self.left_to_read -= (this_write_end - last_write_end)

            if left_to_write >= self.chunk_size - self.current_size and (self.left_to_read >= self.absolute_minimum_part_size):
                self.chunk_done()

            left_to_write -= this_write_end - last_write_end
            last_write_end = this_write_end

        return total_size

    def chunk_done(self, finish=False):
        self.chunk_done_callback(self.current_stream, self.current_size, self.current_hash.hexdigest())
        self.current_stream.close()
        if not finish:
            self.current_size = 0
            self.current_stream = io.BytesIO()
            self.current_hash = hashlib.sha1()


class UploadingDownloadDestBytes(DownloadDestBytes):

    def __init__(self, chunk_uploader: 'ChunkUploader', chunk_size: int, progress_listener: AbstractProgressListener,
                 total_bytes_to_read: int):
        self.chunk_size = chunk_size
        self.chunk_uploader = chunk_uploader
        self.throttled_bytes_io = ThrottledBytesIO(
            chunk_size=self.chunk_size,
            chunk_done_callback=self.chunk_uploader.upload_chunk,
            progress_listener=progress_listener,
            total_bytes_to_read=total_bytes_to_read,
        )
        super().__init__()

    @contextmanager
    def capture_bytes_context(self):
        yield self.throttled_bytes_io


class ChunkUploader:
    def __init__(self, thread_number: int, part_shas: Dict[int, str], session: B2Session, start_part_number,
                 destination_file_id: str, chunk_size: int, progress_listener: AbstractProgressListener,
                 print_memory_usage: bool):
        self.print_memory_usage = print_memory_usage
        self.thread_number = thread_number
        self.part_shas = part_shas
        self.session = session
        self.next_part_number = start_part_number
        self.destination_file_id = destination_file_id
        self.chunk_size = chunk_size
        self.progress_listener = progress_listener

    def upload_chunk(self, stream: io.BytesIO, size: int, content_sha1: str):
        logger.info(f'thread {self.thread_number} uploading chunk {self.next_part_number}')
        if self.thread_number == 0 and self.print_memory_usage:
            print('memory usage: ', psutil.Process(os.getpid()).memory_info().rss / 1024 ** 2)
        self.part_shas[self.next_part_number] = content_sha1
        stream.seek(0)

        self.session.upload_part(
            self.destination_file_id,
            self.next_part_number + 1,
            min(size, self.chunk_size),
            content_sha1,
            stream,
        )
        logger.info(f'done: thread {self.thread_number} uploaded chunk {self.next_part_number}')
        self.progress_listener.bytes_completed(self.progress_listener.prev_value + min(size, self.chunk_size))
        self.next_part_number += 1


def copy_batch(
        thread_number: int,
        part_shas: Dict[int, str],
        source_api: B2Api,
        destination_bucket: Bucket,
        destination_file_id: str,
        source_file_id: str,
        batch_range: Tuple[int, int],
        chunk_size: int,
        start_part_number: int,
        progress_listener: AbstractProgressListener,
        print_memory_usage: bool,
):
    chunk_uploader = ChunkUploader(
        thread_number,
        part_shas,
        destination_bucket.api.session,
        start_part_number,
        destination_file_id,
        chunk_size,
        progress_listener,
        print_memory_usage,
    )
    download_dest = UploadingDownloadDestBytes(chunk_uploader, chunk_size, progress_listener, batch_range[1] - batch_range[0] + 1)
    read_so_far = 0
    while True:
        try:
            source_api.download_file_by_id(source_file_id, download_dest, range_=(batch_range[0] + read_so_far, batch_range[1]))
        except TruncatedOutput as ex:
            read_so_far += ex.bytes_read
            logger.debug(f'thread {thread_number} download failed, retrying')
        else:
            break
    logger.info(
        f'thread {thread_number} finished downloading, leftover: {download_dest.throttled_bytes_io.current_size}')
    if download_dest.throttled_bytes_io.current_size:
        download_dest.throttled_bytes_io.chunk_done(True)


if __name__ == '__main__':
    main()
