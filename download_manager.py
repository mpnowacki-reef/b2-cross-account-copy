from b2sdk.v1 import *

from b2sdk.transfer.inbound.download_manager import DownloadManager
from b2sdk.transfer.inbound.downloader.simple import SimpleDownloader


class SimpleDownloadManager(DownloadManager):
    def __init__(self, services):
        super().__init__(services)
        self.strategies = [
            SimpleDownloader(
                min_chunk_size=self.MIN_CHUNK_SIZE,
                max_chunk_size=self.MAX_CHUNK_SIZE,
            ),
        ]


class SimpleDownloaderB2Api(B2Api):
    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.services.download_manager = SimpleDownloadManager(self.services)
