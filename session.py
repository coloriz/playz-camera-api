import asyncio
from datetime import datetime
from io import BytesIO
from typing import Optional, NoReturn

from picamera import PiCamera, PiCameraAlreadyRecording, PiCameraNotRecording

from util import Event, MediaContainer, Singleton, log


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
        self._in_progress = False
        self._disposed = False
        self._cam = camera
        self._uid = uid
        self._sid = sid
        self._raw_stream = BytesIO()
        self._items = asyncio.Queue()
        self._task_image_capture = None
        self._video_recording_started_at = None
        self._video_mime_type = None
        self._image_mime_type = None

        self.on_stopping = Event()
        self.on_disposed = Event()

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
        self._video_recording_started_at = datetime.now()
        self._cam.start_recording(self._raw_stream, video_format, **kwargs)
        self._cam.wait_recording(0)
        log.debug(f'{self}: Video recording started at {self._video_recording_started_at.isoformat()}. (MIME type: {self._video_mime_type})')
        # Start continuous captures
        self._image_mime_type = f'image/{image_format}'
        self._task_image_capture = asyncio.create_task(self._capture_images(image_capture_interval, image_format))
        log.debug(f'{self}: Continuous image capturing started. (MIME type: {self._image_mime_type})')
        log.info(f'{self}: Session has started.')
        self._in_progress = True

    def stop(self) -> asyncio.Queue:
        """Stop recording and return items"""
        log.info(f'{self}: Attempting to stop...')
        if self._disposed:
            log.error(f'{self} is already been disposed!')
            raise SessionInvalidError
        if not self._in_progress:
            log.error(f'{self} is not in progress!')
            raise PiCameraNotRecording
        # Fire on_stopping event
        self.on_stopping()
        # Stop video recording
        self._cam.stop_recording()
        # Stop image capturing
        self._task_image_capture.cancel()
        # Put the recorded video into the queue.
        self._items.put_nowait(
            MediaContainer(self._raw_stream.getvalue(), self._video_mime_type, self._video_recording_started_at, float(self._cam.framerate))
        )
        self._in_progress = False
        self.dispose()

        return self._items

    async def _capture_images(self, interval: float, image_format: str) -> NoReturn:
        try:
            stream = BytesIO()
            for _ in self._cam.capture_continuous(stream, format=image_format, use_video_port=True):
                captured_at = datetime.now()
                filesize = stream.truncate()
                stream.seek(0)
                self._items.put_nowait(MediaContainer(stream.getvalue(), self._image_mime_type, captured_at))
                log.debug(f'{self}: A new image captured at {captured_at.isoformat()}. ({filesize} bytes)')

                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            log.debug(f'{self}: Gracefully stop continous capturing.')
            log.debug(f'{self}: Total {self._items.qsize()} images captured.')

        log.debug(f'{self}: Image capturing task finished.')

    @property
    def is_running(self) -> bool:
        return self._in_progress

    def dispose(self) -> NoReturn:
        if self._disposed:
            return
        if self._in_progress:
            self.stop()
            return
        # Dispose this session
        self._disposed = True
        # Fire on_disposed event
        self.on_disposed()
        log.info(f'{self} has been disposed.')

    def __del__(self) -> NoReturn:
        self.dispose()


class SessionManager(metaclass=Singleton):
    def __init__(self, camera: PiCamera, session_timeout: float = 60.0) -> NoReturn:
        self._cam: PiCamera = camera
        self.__instance: Optional[Session] = None
        self._timeout: float = session_timeout
        self._task_watchdog: Optional[asyncio.Task] = None

    def _empty_session(self) -> NoReturn:
        self.__instance = None

    async def _timeout_watchdog(self):
        log.debug(f'Timeout watchdog initialized. Automatically destroy the session in {self._timeout} seconds')
        try:
            await asyncio.sleep(self._timeout)
            self.destroy_silently()
            log.warning('Watchdog invoked. The session has been destroyed.')
        except asyncio.CancelledError:
            log.debug('Watchdog cancelled.')

    def _cancel_watchdog(self):
        if self._task_watchdog:
            self._task_watchdog.cancel()
            self._task_watchdog = None

    def create(self, *args, **kwargs) -> Session:
        if self.__instance:
            raise SessionAlreadyExists('A session can only exist only one at any given time.')
        self.__instance = Session(self._cam, *args, **kwargs)
        self.__instance.on_stopping.attach(self._cancel_watchdog)
        self.__instance.on_disposed.attach(self._empty_session)
        self._task_watchdog = asyncio.create_task(self._timeout_watchdog())
        return self.__instance

    def destroy(self) -> NoReturn:
        if not self.__instance:
            raise SessionNotExists
        self.__instance.dispose()

    def destroy_silently(self) -> NoReturn:
        try:
            self.destroy()
        except SessionNotExists:
            pass

    @property
    def session(self) -> Session:
        if not self.__instance:
            raise SessionNotExists
        return self.__instance
