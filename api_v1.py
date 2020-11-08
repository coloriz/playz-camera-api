import asyncio
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

from picamera import PiCamera, PiCameraAlreadyRecording
from aiohttp import web, ClientSession, FormData

from session import SessionAlreadyExists, SessionNotExists, SessionManager
from util import convert_raw_video_to_mp4_stream, ItemUploader


async def capture_image(cam: PiCamera, delay: float, image_format='jpeg'):
    """Capture an image"""
    # Insert a delay before taking an image
    if delay > 0:
        await asyncio.sleep(delay)

    # Trigger a shutter
    stream = BytesIO()
    cam.capture(stream, image_format, use_video_port=True)

    return stream.getvalue()


async def capture_video(cam: PiCamera, delay: float, timeout: float, video_format='h264', **kwargs):
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

    # Convert the recorded raw H.264 video into .mp4
    mp4_stream = await convert_raw_video_to_mp4_stream(raw_stream.getvalue(), float(cam.framerate))

    return mp4_stream


async def upload_to_server(endpoint: str, token: str, path: Path, data: bytes, content_type: str):
    form = FormData()
    form.add_field('token', token)
    # Prepend "/" to path
    form.add_field('upload_path', f'/{path}')
    form.add_field('upload', data, content_type=content_type, filename=path.name)

    # Upload the data to the server asyncly
    async with ClientSession() as session:
        async with session.post(endpoint, data=form) as res:
            pass


async def capture_image_and_upload(cam: PiCamera, delay: float, endpoint: str, token: str, path: Path):
    """Capture an image and upload"""
    data = await capture_image(cam, delay)
    await upload_to_server(endpoint, token, path, data, 'image/jpeg')


async def capture_video_and_upload(
        cam: PiCamera, delay: float, timeout: float, endpoint: str, token: str, path: Path, **kwargs):
    """Capture a video and upload"""
    data = await capture_video(cam, delay, timeout, **kwargs)
    await upload_to_server(endpoint, token, path, data, 'video/mp4')


def error_response(error: Exception, msg: Optional[str] = None, code: int = 400):
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
    try:
        params = await request.json()
        uid = int(params['uid'])
        entry_datetime = str(params['entry_datetime'])
        assert entry_datetime.isdigit(), "'entry_datetime' should be the form of 'YYYYmmddHHMMSS'"
        delay = float(params.get('delay', config['delay']))
        timeout = float(params.get('timeout', config['timeout']))
        mode = params.get('mode', 'video')
        if mode == 'image':
            path = Path(f'{uid}/{entry_datetime}/{config["module_id"]}-{datetime.now().strftime("%Y%m%d%H%M%S")}.jpg')
            coro = capture_image_and_upload(cam, delay, config['upload_endpoint'], config['token'], path)
        elif mode == 'video':
            path = Path(f'{uid}/{entry_datetime}/{config["module_id"]}-{datetime.now().strftime("%Y%m%d%H%M%S")}.mp4')
            coro = capture_video_and_upload(cam, delay, timeout, config['upload_endpoint'], config['token'], path,
                                            level='4.2', bitrate=config['bitrate'], quality=config['quality'])
        else:
            raise ValueError(f"Unknown 'mode': {mode}. should be either 'image' or 'video'.")
    except KeyError as e:
        return error_response(e, f'A required key {e} is missing.')
    except Exception as e:
        return error_response(e)

    # Run tasks asyncly
    asyncio.create_task(coro)

    return web.json_response({'uri': urljoin(config['upload_root'], str(path))})


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
        return error_response(e)

    return await handle_get_camera_settings(request)


@routes.get('/session')
async def handle_get_session(request: web.Request):
    config = request.config_dict
    cam: PiCamera = config['camera']
    session_manager: SessionManager = config['session_manager']
    uploader: ItemUploader = config['uploader']
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
            items = await session.stop()
            # Upload items
            upload_path = Path(f'{session.uid}/{session.sid}/')
            asyncio.create_task(uploader.upload(upload_path, items))
            return web.json_response({
                'session': str(session),
                'uri': urljoin(config['upload_root'], str(upload_path))
            })
        elif cmd == 'interrupt':
            session = session_manager.session
            await session_manager.destroy()
            return web.json_response({'session': str(session)})
        else:
            raise ValueError(f"Unknown cmd={cmd}. should be one of [enter, exit, interrupt].")
    except KeyError as e:
        return error_response(e, f'A required parameter {e} is missing.')
    except PiCameraAlreadyRecording as e:
        await session_manager.destroy_silently()
        return error_response(e, 'The camera is busy.', 429)
    except SessionAlreadyExists as e:
        return error_response(e, 'Running session already exists.', code=409)
    except SessionNotExists as e:
        return error_response(e, 'No running session.', code=404)
    except Exception as e:
        return error_response(e)

    # Code never reaches here
    raise web.HTTPInternalServerError()


api_v1 = web.Application()
api_v1.add_routes(routes)
