import asyncio
import logging
from datetime import datetime
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryFile
from typing import NamedTuple, Optional, NoReturn, BinaryIO, Tuple, Iterable, Union

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


async def containerize_raw_video(raw_stream: Union[BinaryIO, BytesIO],
                                 framerate: float,
                                 fmt: str,
                                 extra_options: Optional[Iterable[str]] = None,
                                 ffmpeg_bin: str = 'ffmpeg') -> BinaryIO:
    video = TemporaryFile()
    cmd = [
        ffmpeg_bin,
        '-hide_banner',
        '-loglevel', 'error',
        '-framerate', f'{framerate}',
        '-i', '-',
        '-an',
        '-c:v', 'copy',
        '-f', fmt
    ]
    if extra_options:
        cmd.extend(extra_options)
    cmd.extend(['-'])

    bytesio_input = isinstance(raw_stream, BytesIO)

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdin=raw_stream if not bytesio_input else asyncio.subprocess.PIPE,
        stdout=video,
        stderr=asyncio.subprocess.PIPE
    )

    if bytesio_input:
        proc.stdin.write(raw_stream.getvalue())
        proc.stdin.close()

    # Wait for process to terminate
    _, err = await proc.communicate()
    if err:
        log.error(f'FFmpeg error: {err.decode()}')

    video.seek(0)
    # Issue: aiohttp FormData serializer doesn't detect TemporaryFileWrapper(NamedTemporaryFile) as IOBase.
    return video


def bytes_for_humans(n: int) -> str:
    for unit in ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB']:
        if n < 1024.0:
            return f'{n:.1f}{unit}'
        n /= 1024.0
    return f'{n:.1f}YB'


class MediaContainer(NamedTuple):
    file: BinaryIO
    mimetype: str
    timestamp: datetime
    framerate: Optional[float] = None


class MediaUploader(metaclass=Singleton):
    def __init__(self, upload_endpoint: str, token: str, workers: int = 4) -> NoReturn:
        self._upload_endpoint = upload_endpoint
        self._token = token

        # Create worker tasks to process the queue concurrently.
        self._tasks = [asyncio.create_task(self._worker(f'worker-{i}')) for i in range(workers)]
        self._q: "asyncio.Queue[Tuple[Path, MediaContainer]]" = asyncio.Queue()

    def put(self, upload_path: Path, item: MediaContainer) -> NoReturn:
        self._q.put_nowait((upload_path, item))

    async def dispose(self) -> NoReturn:
        # Wait until the queue is fully processed.
        await self._q.join()

        # Cancel our worker tasks.
        for task in self._tasks:
            task.cancel()

        # Wait until all worker tasks are cancelled.
        await asyncio.gather(*self._tasks, return_exceptions=True)

    async def _worker(self, name: str) -> NoReturn:
        session = ClientSession()
        try:
            while True:
                # Get a work item out of the queue.
                upload_path, media = await self._q.get()
                file = media.file
                mimetype = media.mimetype
                framerate = media.framerate

                if mimetype.startswith('video/'):
                    # Convert to mp4
                    file = await containerize_raw_video(file, framerate, 'mp4', ['-movflags', 'empty_moov'])

                # Get the size of a file object
                file.seek(0, 2)
                filesize = file.tell()
                file.seek(0)

                form = FormData()
                form.add_field('token', self._token)
                # Prepend "/" to path
                form.add_field('upload_path', f'/{upload_path}')
                form.add_field('upload', file, content_type=mimetype, filename=upload_path.name)

                log.info(f'{name}: upload_path: /{upload_path}, size: {bytes_for_humans(filesize)}')

                async with session.post(self._upload_endpoint, data=form) as res:
                    log.info(f'{name}: Response from the server: {repr(res)}, {await res.text()}')

                # Notify the queue that the work item has been processed.
                self._q.task_done()
        finally:
            await session.close()
