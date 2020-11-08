import asyncio
import logging
from datetime import datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import NamedTuple, Optional, NoReturn, Iterable, BinaryIO

from aiohttp import ClientSession, FormData

log = logging.getLogger('playz-module-camera')


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        else:
            cls._instances[cls].__init__(*args, **kwargs)
        return cls._instances[cls]


class Event:
    def __init__(self):
        self._handlers = []

    def attach(self, handler) -> NoReturn:
        self._handlers.append(handler)

    def detach(self, handler) -> NoReturn:
        self._handlers.remove(handler)

    async def __call__(self, *args, **kwargs) -> NoReturn:
        for handler in self._handlers:
            await handler(*args, **kwargs)


async def convert_raw_video_to_mp4_stream(raw_stream: BinaryIO,
                                          framerate: float,
                                          ffmpeg_bin: str = 'ffmpeg') -> BinaryIO:
    mp4_stream = NamedTemporaryFile()
    proc = await asyncio.create_subprocess_exec(
        ffmpeg_bin,
        '-hide_banner',
        '-loglevel', 'error',
        '-framerate', f'{framerate}',
        '-i', '-',
        '-an',
        '-c:v', 'copy',
        '-f', 'mp4',
        # '-movflags', 'empty_moov',
        '-y',
        mp4_stream.name,
        stdin=raw_stream,
        stderr=None
    )
    await proc.wait()

    return mp4_stream


def bytes_for_humans(n: int) -> str:
    for unit in ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB']:
        if n < 1024.0:
            return f'{n:.1f}{unit}'
        n /= 1024.0
    return f'{n:.1f}YB'


class MediaContainer(NamedTuple):
    file: BinaryIO
    mimetype: str
    captured_at: datetime
    framerate: Optional[float] = None


class MediaUploader(metaclass=Singleton):
    def __init__(self, upload_endpoint: str, module_id: str, token: str, workers: int = 4) -> NoReturn:
        self._upload_endpoint = upload_endpoint
        self._module_id = module_id
        self._token = token

        # Create worker tasks to process the queue concurrently.
        self._tasks = [asyncio.create_task(self._worker(f'worker-{i}')) for i in range(workers)]
        self._q = asyncio.Queue()

    def put_items(self, upload_path: Path, items: Iterable[MediaContainer]) -> NoReturn:
        for item in items:
            self._q.put_nowait((upload_path, item))

    async def dispose(self) -> NoReturn:
        # Wait until the queue is fully processed.
        await self._q.join()

        # Cancel our worker tasks.
        for task in self._tasks:
            task.cancel()

        # Wait until all worker tasks are cancelled.
        await asyncio.gather(*self._tasks, return_exceptions=True)

    def make_filename(self, captured_at: datetime, ext: str) -> str:
        return f'{self._module_id}-{captured_at.strftime("%Y%m%d%H%M%S")}{ext}'

    async def _worker(self, name: str) -> NoReturn:
        session = ClientSession()
        try:
            while True:
                # Get a work item out of the queue.
                upload_path, media = await self._q.get()
                file = media.file
                mimetype = media.mimetype
                captured_at = media.captured_at

                ext = ''
                if media.mimetype.startswith('image/'):
                    subtype = media.mimetype[6:].lower()
                    if subtype == 'jpeg':
                        ext = '.jpg'
                    elif subtype == 'png':
                        ext = '.png'
                elif media.mimetype.startswith('video/'):
                    ext = '.mp4'
                    # Convert to mp4
                    file = await convert_raw_video_to_mp4_stream(file, media.framerate)

                # Check if ext is set
                if not ext:
                    log.warning('ext is not set!')

                filename = self.make_filename(captured_at, ext)
                upload_path_full = f'/{upload_path / filename}'

                # Get the size of a file object
                file.seek(0, 2)
                filesize = file.tell()
                file.seek(0)

                form = FormData()
                form.add_field('token', self._token)
                # Prepend "/" to path
                form.add_field('upload_path', upload_path_full)
                form.add_field('upload', file, content_type=mimetype, filename=filename)

                log.info(f'{name}: upload_path: {upload_path_full}, size: {bytes_for_humans(filesize)}')

                async with session.post(self._upload_endpoint, data=form) as res:
                    log.info(f'{name}: Response from the server: {repr(res)}, {await res.text()}')

                # Notify the queue that the work item has been processed.
                self._q.task_done()
        finally:
            await session.close()
