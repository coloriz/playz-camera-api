import asyncio
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import NamedTuple, Optional, NoReturn

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

    def attach(self, handler):
        self._handlers.append(handler)

    def detach(self, handler):
        self._handlers.remove(handler)

    def __call__(self, *args, **kwargs):
        for handler in self._handlers:
            handler(*args, **kwargs)


async def convert_raw_video_to_mp4_stream(raw_stream: bytes, framerate: float, ffmpeg_bin: str = 'ffmpeg') -> bytes:
    proc = await asyncio.create_subprocess_exec(
        ffmpeg_bin,
        '-hide_banner',
        '-loglevel', 'error',
        '-framerate', f'{framerate}',
        '-i', '-',
        '-an',
        '-c:v', 'copy',
        '-f', 'mp4',
        '-movflags', 'empty_moov',
        '-',
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=None
    )
    mp4_stream, _ = await proc.communicate(raw_stream)

    return mp4_stream


class MediaContainer(NamedTuple):
    data: bytes
    mimetype: str
    captured_at: datetime
    framerate: Optional[float] = None


class ItemUploader:
    def __init__(self, upload_endpoint: str, module_id: str, token: str) -> NoReturn:
        self._upload_endpoint = upload_endpoint
        self._module_id = module_id
        self._token = token

    async def upload(self, upload_path: Path, q: asyncio.Queue, workers: int = 4):
        # Create worker tasks to process the queue concurrently.
        tasks = []
        for i in range(workers):
            task = asyncio.create_task(self._worker(upload_path, q, name=f'worker-{i}'))
            tasks.append(task)

        # Wait until the queue is fully processed.
        await q.join()

        # Cancel our worker tasks.
        for task in tasks:
            task.cancel()

        # Wait until all worker tasks are cancelled.
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _worker(self, upload_path: Path, q: asyncio.Queue, name: str):
        session = ClientSession()
        try:
            while True:
                # Get a work item out of the queue.
                media = await q.get()
                data = media.data
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
                    data = await convert_raw_video_to_mp4_stream(media.data, media.framerate)

                # Check if ext is set
                if not ext:
                    log.warning('ext is not set!')

                filename = f'{self._module_id}-{captured_at.strftime("%Y%m%d%H%M%S")}{ext}'
                upload_path_full = f'/{upload_path / filename}'

                form = FormData()
                form.add_field('token', self._token)
                # Prepend "/" to path
                form.add_field('upload_path', upload_path_full)
                form.add_field('upload', data, content_type=mimetype, filename=filename)

                log.info(f'{name}: filename: {filename}, upload_path: {upload_path_full}, size: {len(data)}')

                async with session.post(self._upload_endpoint, data=form) as res:
                    log.info(f'{name}: Response from the server: {repr(res)}')
                    log.info(f'{name}: {await res.text()}')

                # Notify the queue that the work item has been processed.
                q.task_done()
        finally:
            await session.close()
