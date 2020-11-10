import asyncio
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Optional, BinaryIO
from urllib.parse import urljoin

from picamera import PiCamera, PiCameraAlreadyRecording
from aiohttp import web

from session import SessionAlreadyExists, SessionNotExists, SessionManager
from util import MediaUploader, MediaContainer


async def capture_image(cam: PiCamera, delay: float, image_format='jpeg') -> BinaryIO:
    """Capture an image"""
    # Insert a delay before taking an image
    if delay > 0:
        await asyncio.sleep(delay)

    # Trigger a shutter
    stream = BytesIO()
    cam.capture(stream, image_format, use_video_port=True)
    stream.seek(0)

    return stream


async def capture_video(cam: PiCamera, delay: float, timeout: float, video_format='h264', **kwargs) -> BinaryIO:
    """Capture a video"""
    # Insert a delay before recording
    if delay > 0:
        await asyncio.sleep(delay)

    # Start recording
    raw_stream = BytesIO()
    cam.start_recording(raw_stream, video_format, **kwargs)
    cam.wait_recording(0)
    await asyncio.sleep(timeout)
    cam.stop_recording()
    raw_stream.seek(0)

    return raw_stream


async def capture_image_and_upload(cam: PiCamera, delay: float,
                                   uploader: MediaUploader, upload_path: Path, captured_at: datetime):
    """Capture an image and upload"""
    stream = await capture_image(cam, delay, 'jpeg')
    uploader.put_items(upload_path, [MediaContainer(stream, 'image/jpeg', captured_at)])


async def capture_video_and_upload(cam: PiCamera, delay: float, timeout: float,
                                   uploader: MediaUploader, upload_path: Path, captured_at: datetime,
                                   **kwargs):
    """Capture a video and upload"""
    stream = await capture_video(cam, delay, timeout, 'h264', **kwargs)
    uploader.put_items(upload_path, [MediaContainer(stream, 'video/H264', captured_at, float(cam.framerate))])


def error_response(error: Exception, msg: Optional[str] = None, code: int = 500):
    if not msg:
        msg = str(error)
    return web.json_response({'msg': msg, 'type': type(error).__name__, 'code': code}, status=code)


def assert_camera_idle(f):
    def inner(request: web.Request):
        cam: PiCamera = request.config_dict['camera']
        if cam.recording:
            return error_response(PiCameraAlreadyRecording('The camera is busy.'), code=429)
        return f(request)

    return inner


routes = web.RouteTableDef()


@routes.get('/camera')
async def handle_get_camera(request: web.Request):
    cam: PiCamera = request.config_dict['camera']
    return web.json_response({'recording': cam.recording})


@routes.post('/camera')
@assert_camera_idle
async def handle_post_camera(request: web.Request):
    config = request.config_dict
    cam: PiCamera = config['camera']
    uploader: MediaUploader = config['uploader']
    try:
        params = await request.json()
        uid = int(params['uid'])
        entry_datetime = str(params['entry_datetime'])
        assert entry_datetime.isdigit(), "'entry_datetime' should be the form of 'YYYYmmddHHMMSS'"
        delay = float(params.get('delay', config['delay']))
        timeout = float(params.get('timeout', config['timeout']))
        mode = params.get('mode', 'video')
        upload_path = Path(f'{uid}/{entry_datetime}/')
        captured_at = datetime.now()
        if mode == 'image':
            coro = capture_image_and_upload(cam, delay, uploader, upload_path, captured_at)
            filename = uploader.make_filename(captured_at, '.jpg')
        elif mode == 'video':
            coro = capture_video_and_upload(cam, delay, timeout, uploader, upload_path, captured_at,
                                            level='4.2', bitrate=config['bitrate'], quality=config['quality'])
            filename = uploader.make_filename(captured_at, '.mp4')
        else:
            raise ValueError(f"Unknown 'mode': {mode}. should be either 'image' or 'video'.")
    except KeyError as e:
        return error_response(e, f'A required key {e} is missing.', 400)
    except Exception as e:
        return error_response(e)

    # Run tasks asyncly
    asyncio.create_task(coro)

    return web.json_response({'uri': urljoin(config['upload_root'], str(upload_path / filename))})


@routes.get('/camera/settings')
async def handle_get_camera_settings(request: web.Request):
    cam: PiCamera = request.config_dict['camera']
    width, height = cam.resolution
    return web.json_response({
        'width': width,
        'height': height,
        'framerate': float(cam.framerate),
        'rotation': cam.rotation,
        'exposure_mode': cam.exposure_mode,
    })


@routes.put('/camera/settings')
@assert_camera_idle
async def handle_put_camera_settings(request: web.Request):
    """Update camera settings"""
    # Back up original settings
    cam: PiCamera = request.config_dict['camera']
    _resolution = cam.resolution
    _framerate = cam.framerate
    _rotation = cam.rotation
    _exposure_mode = cam.exposure_mode
    try:
        settings = await request.json()
        assert isinstance(settings['width'], int) and 640 <= settings['width'] <= 3280
        assert isinstance(settings['height'], int) and 480 <= settings['height'] <= 2464
        assert 0 <= settings['framerate'] <= 90
        assert isinstance(settings['rotation'], int) and settings['rotation'] in [0, 90, 180, 270]
        assert settings['exposure_mode'] in PiCamera.EXPOSURE_MODES.keys()

        cam.resolution = settings['width'], settings['height']
        cam.framerate = settings['framerate']
        cam.rotation = settings['rotation']
        cam.exposure_mode = settings['exposure_mode']
    except Exception as e:
        cam.resolution = _resolution
        cam.framerate = _framerate
        cam.rotation = _rotation
        cam.exposure_mode = _exposure_mode
        return error_response(e, code=400)

    return await handle_get_camera_settings(request)


@routes.get('/session')
async def handle_get_session(request: web.Request):
    config = request.config_dict
    cam: PiCamera = config['camera']
    session_manager: SessionManager = config['session_manager']
    uploader: MediaUploader = config['uploader']
    try:
        cmd = request.query['cmd']
        if cmd == 'enter':
            uid = int(request.query['uid'])
            entry_datetime = request.query['entry_datetime']
            assert entry_datetime.isdigit(), "'entry_datetime' should be the form of 'YYYYmmddHHMMSS'"
            capture_interval = float(request.query.get('capture_interval', config['capture_interval']))

            session = session_manager.create(cam, uid, entry_datetime)
            session.start(capture_interval, 'jpeg', 'h264',
                          level='4.2', bitrate=config['bitrate'], quality=config['quality'])
            return web.json_response({'session': str(session)}, status=201)
        elif cmd == 'exit':
            session = session_manager.session
            await session_manager.destroy(upload=True)
            # Upload items
            upload_path = Path(f'{session.uid}/{session.sid}/')
            return web.json_response({
                'session': str(session),
                'uri': urljoin(config['upload_root'], str(upload_path))
            })
        elif cmd == 'interrupt':
            session = session_manager.session
            await session_manager.destroy(upload=False)
            return web.json_response({'session': str(session)})
        else:
            raise ValueError(f"Unknown cmd={cmd}. should be one of [enter, exit, interrupt].")
    except KeyError as e:
        return error_response(e, f'A required parameter {e} is missing.', 400)
    except PiCameraAlreadyRecording as e:
        await session_manager.destroy_silently()
        return error_response(e, 'The camera is busy.', 429)
    except SessionAlreadyExists as e:
        return error_response(e, 'Running session already exists.', 409)
    except SessionNotExists as e:
        return error_response(e, 'No running session.', 404)
    except Exception as e:
        return error_response(e)

    # Code never reaches here
    raise web.HTTPInternalServerError()


api_v1 = web.Application()
api_v1.add_routes(routes)
