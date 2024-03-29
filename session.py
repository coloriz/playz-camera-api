import asyncio
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryFile
from time import timezone
from typing import Optional, NoReturn, List, Tuple

from picamera import PiCamera, PiCameraAlreadyRecording, PiCameraNotRecording

from util import Event, MediaContainer, Singleton, log, bytes_for_humans, MediaUploader


class SessionAlreadyExists(Exception):
    """
    A session can only exist only one at any given time.
    This exception is raised when trying to create multiple sessions.
    """


class SessionNotExists(Exception):
    """
    Raised when there is no session in SessionManager.
    """


class SessionInvalidError(Exception):
    """
    Raised when an operation is attempted on the disposed session.
    """


class Session:
    def __init__(self, camera: PiCamera, uid: int, sid: str) -> NoReturn:
        # Indicate the status of this session
        self._in_progress = False
        self._disposed = False

        self._cam = camera
        self._uid = uid
        self._sid = sid

        self._raw_stream = TemporaryFile()
        self._items = []

        self._task_image_capture = None
        self._started_at = None
        self._video_mime_type = None
        self._image_mime_type = None

        self.on_stopping = Event()  # Invoked just before stopping
        self.on_disposed = Event()  # Invoked after fully disposed

        # Get local timezone to properly print timestamps
        self._tz = datetime.timezone(datetime.timedelta(seconds=-timezone))

    def __str__(self) -> str:
        return f"Session(" \
               f"uid={self._uid}, sid='{self._sid}', " \
               f"in_progress={self._in_progress}, disposed={self._disposed})"

    @property
    def uid(self) -> int:
        return self._uid

    @property
    def sid(self) -> str:
        return self._sid

    def start(self,
              image_capture_interval: float, image_format: str = 'jpeg',
              video_format: str = 'h264', **kwargs) -> NoReturn:
        """Start a new session."""
        log.info(f'{self}: Attemping to start a new session...')
        if self._disposed:
            log.error(f'{self} is already been disposed!')
            raise SessionInvalidError
        if self._in_progress:
            log.error(f'{self} is already in progress.')
            raise PiCameraAlreadyRecording
        # Start recording a video
        self._video_mime_type = f'video/{video_format.upper()}'
        self._started_at = datetime.datetime.now(self._tz)
        self._cam.start_recording(self._raw_stream, video_format, **kwargs)
        # Check if any error occured
        self._cam.wait_recording(0)
        log.debug(f'Video recording started at {self._started_at.isoformat()}. (MIME type: {self._video_mime_type})')

        # Start continuous captures
        self._image_mime_type = f'image/{image_format}'
        self._task_image_capture = asyncio.create_task(self._capture_images(image_capture_interval, image_format))
        log.debug(f'Continuous image capturing started. (MIME type: {self._image_mime_type})')

        self._in_progress = True
        log.info(f'{self}: Session has started.')

    async def stop(self) -> List[MediaContainer]:
        """Stop recording and return items"""
        log.info(f'{self}: Attempting to stop...')
        if self._disposed:
            log.error(f'{self} is already been disposed!')
            raise SessionInvalidError
        if not self._in_progress:
            log.error(f'{self} is not in progress!')
            raise PiCameraNotRecording
        # Fire on_stopping event
        await self.on_stopping()

        # Stop video recording
        self._cam.stop_recording()
        log.debug(f'Video recording stopped.')

        # Stop image capturing
        self._task_image_capture.cancel()
        await self._task_image_capture
        log.debug(f'Image capturing stopped.')
        log.info(f'Total {len(self._items)} images captured.')

        # Put the recorded video into the list.
        self._raw_stream.seek(0)
        self._items.append(
            MediaContainer(
                self._raw_stream,
                self._video_mime_type,
                self._started_at,
                float(self._cam.framerate))
        )

        self._in_progress = False
        await self.dispose()

        return self._items

    async def _capture_image_on_event(self, event: asyncio.Event, image_format: str) -> NoReturn:
        try:
            while True:
                await event.wait()
                stream = TemporaryFile()
                self._cam.capture(stream, image_format, use_video_port=True)
                timestamp = datetime.datetime.now(self._tz)
                filesize = stream.tell()
                stream.seek(0)
                self._items.append(MediaContainer(stream, self._image_mime_type, timestamp))
                log.debug(f'New image captured at {timestamp.isoformat()}. ({bytes_for_humans(filesize)})')
                event.clear()
        except asyncio.CancelledError:
            pass

    async def _capture_images(self, interval: float, image_format: str) -> NoReturn:
        event = asyncio.Event()
        task = asyncio.create_task(self._capture_image_on_event(event, image_format))
        try:
            while True:
                event.set()
                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            log.debug('Cancel signal received. Gracefully stopping continous image capturing...')
            task.cancel()
            await task

    @property
    def is_running(self) -> bool:
        return self._in_progress

    async def dispose(self) -> NoReturn:
        if self._disposed:
            return
        if self._in_progress:
            await self.stop()
            return
        # Dispose this session
        self._disposed = True
        # Fire on_disposed event
        await self.on_disposed()
        log.info(f'{self} has been disposed.')


class SessionManager(metaclass=Singleton):
    def __init__(self, session_timeout: float, uploader: MediaUploader, path_fmt: str) -> NoReturn:
        self.__instance: Optional[Session] = None
        self._lock: asyncio.Lock = asyncio.Lock()
        self._timeout: float = session_timeout
        self._task_watchdog: Optional[asyncio.Task] = None
        self._uploader: MediaUploader = uploader
        self._path_fmt: str = path_fmt

    async def _empty_session(self) -> NoReturn:
        self.__instance = None

    async def _timeout_watchdog(self) -> NoReturn:
        log.debug(f'Timeout watchdog initialized. Automatically destroy the session in {self._timeout} seconds')
        try:
            await asyncio.sleep(self._timeout)
            # Watchdog invoked. No coming back.
            self.__instance.on_stopping.detach(self._cancel_watchdog)
            await self.destroy(True)
            log.warning('Watchdog invoked. The session has been destroyed.')
        except SessionNotExists:
            log.warning("Watchdog invoked. But the session doesn't exist. Skipping...")
        except asyncio.CancelledError:
            log.debug('Watchdog cancelled.')

    async def _cancel_watchdog(self) -> NoReturn:
        if self._task_watchdog:
            self._task_watchdog.cancel()
            await self._task_watchdog
            self._task_watchdog = None

    def create(self, *args, **kwargs) -> Session:
        if self.__instance:
            raise SessionAlreadyExists('A session can only exist only one at any given time.')
        self.__instance = Session(*args, **kwargs)
        self.__instance.on_stopping.attach(self._cancel_watchdog)
        self.__instance.on_disposed.attach(self._empty_session)
        self._task_watchdog = asyncio.create_task(self._timeout_watchdog())
        return self.__instance

    async def destroy(self, upload: bool = True) -> Tuple[Session, List[Path]]:
        async with self._lock:
            session = self.__instance
            if not session:
                raise SessionNotExists
            items = await self.__instance.stop()

        path_list = []
        if upload:
            # Upload items
            fmt = self._path_fmt
            for item in items:
                # Determine a file extension based on its mimetype
                ext = ''
                if item.mimetype.startswith('image/'):
                    subtype = item.mimetype[6:].lower()
                    if subtype == 'jpeg':
                        ext = '.jpg'
                    elif subtype == 'png':
                        ext = '.png'
                elif item.mimetype.startswith('video/'):
                    ext = '.mp4'

                # Check if ext is set
                if not ext:
                    log.warning('ext is not set!')

                upload_path = Path(fmt.format(uid=session.uid, sid=session.sid, timestamp=item.timestamp, ext=ext))
                path_list.append(upload_path)
                # Put the item in the uploader's queue
                self._uploader.put(upload_path, item)

        return session, path_list

    async def destroy_silently(self) -> NoReturn:
        try:
            await self.destroy(False)
        except SessionNotExists:
            pass

    @property
    def session(self) -> Session:
        if not self.__instance:
            raise SessionNotExists
        return self.__instance
