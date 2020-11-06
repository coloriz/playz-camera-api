import asyncio
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from datetime import datetime
from io import BytesIO
import logging
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

from aiohttp import web, ClientSession, FormData
from picamera import PiCamera, PiCameraAlreadyRecording

from session import SessionManager, SessionAlreadyExists, SessionNotExists
from util import ItemUploader

parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter, add_help=False)
parser.add_argument('--help', action='help', help='show this help message and exit')
parser.add_argument('-w', '--width', default=1640, type=int, help='set image width')
parser.add_argument('-h', '--height', default=1232, type=int, help='set image height')
parser.add_argument('-fps', '--framerate', default=40, type=float, help='specify the frames per second to record')
parser.add_argument('-rot', '--rotation', default=0, type=int, choices=[0, 90, 180, 270], help='set image rotation')
parser.add_argument('-em', '--exposure-mode', default='sports', choices=PiCamera.EXPOSURE_MODES.keys(),
                    help='set the exposure mode')
parser.add_argument('-b', '--bitrate', default=6000000, type=int, help='set bitrate')
parser.add_argument('-q', '--quality', default=17, type=int,
                    help='the quality that the encoder should attempt to maintain')
parser.add_argument('-d', '--delay', default=0, type=float, help='default delay before recording (in second)')
parser.add_argument('-t', '--timeout', default=5, type=float, help='default time to capture for (in second)')
parser.add_argument('-ci', '--capture-interval', default=5, type=float,
                    help='default image capture interval during a session (in seconds)')
parser.add_argument('--upload-endpoint', default='')
parser.add_argument('--upload-root', default='')
parser.add_argument('--token', default='')
parser.add_argument('--module-id', default='01')
parser.add_argument('--request-port', default=8080, type=int, help='port to listen capture requests')
parser.add_argument('--debug', action='store_true', help='enable debug mode')

opt = parser.parse_args()

if opt.debug:
    logging.basicConfig(level=logging.DEBUG)

# Initialize Pi camera hardware
cam = PiCamera()
cam.resolution = opt.width, opt.height
cam.framerate = opt.framerate
cam.rotation = opt.rotation
cam.exposure_mode = opt.exposure_mode
# One and only session manager
session_manager = SessionManager(cam)
# Item uploader
uploader = ItemUploader(opt.upload_endpoint, opt.module_id, opt.token)


async def capture_image(delay):
    """Capture an image"""
    # Insert a delay before taking an image
    if delay > 0:
        await asyncio.sleep(delay)

    # Trigger a shutter
    stream = BytesIO()
    cam.capture(stream, format='jpeg', use_video_port=True)

    return stream.getvalue()


async def capture_video(delay, timeout):
    """Capture a video"""
    # Insert a delay before recording
    if delay > 0:
        await asyncio.sleep(delay)

    # Start recording
    raw_stream = BytesIO()
    cam.start_recording(raw_stream, format='h264', level='4.2', bitrate=opt.bitrate, quality=opt.quality)
    cam.wait_recording(0)
    await asyncio.sleep(timeout)
    cam.stop_recording()

    # Convert the recorded raw H.264 video into .mp4
    proc = await asyncio.create_subprocess_exec(
        'ffmpeg',
        '-hide_banner',
        '-loglevel', 'error',
        '-framerate', f'{opt.framerate}',
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
    mp4_stream, _ = await proc.communicate(raw_stream.getvalue())

    return mp4_stream


async def upload_to_server(path, data, content_type):
    form = FormData()
    form.add_field('token', opt.token)
    # Prepend "/" to path
    form.add_field('upload_path', f'/{path}')
    form.add_field('upload', data, content_type=content_type, filename=path.name)

    # Upload the data to the server asyncly
    async with ClientSession() as session:
        await session.post(opt.upload_endpoint, data=form)


async def capture_image_and_upload(path, delay):
    """Capture an image and upload"""
    data = await capture_image(delay)
    await upload_to_server(path, data, 'image/jpeg')


async def capture_video_and_upload(path, delay, timeout):
    """Capture a video and upload"""
    data = await capture_video(delay, timeout)
    await upload_to_server(path, data, 'video/mp4')


def error_response(error: Exception, msg: Optional[str] = None, code: int = 400):
    if not msg:
        msg = str(error)
    return web.json_response({'msg': msg, 'type': type(error).__name__, 'code': code}, status=code)


def assert_camera_idle(f):
    def inner(*args, **kwargs):
        if cam.recording:
            return error_response(PiCameraAlreadyRecording('The camera is busy.'), code=429)
        return f(*args, **kwargs)

    return inner


routes = web.RouteTableDef()


@routes.get('/v1/camera')
async def handle_get_camera(request: web.Request):
    return web.json_response({'recording': cam.recording})


@routes.post('/v1/camera')
@assert_camera_idle
async def handle_post_camera(request: web.Request):
    try:
        params = await request.json()
        uid = int(params['uid'])
        entry_datetime = str(params['entry_datetime'])
        assert entry_datetime.isdigit(), "'entry_datetime' should be the form of 'YYYYmmddHHMMSS'"
        delay = float(params.get('delay', opt.delay))
        timeout = float(params.get('timeout', opt.timeout))
        mode = params.get('mode', 'video')
        if mode == 'image':
            path = Path(f'{uid}/{entry_datetime}/{opt.module_id}-{datetime.now().strftime("%Y%m%d%H%M%S")}.jpg')
            coro = capture_image_and_upload(path, delay)
        elif mode == 'video':
            path = Path(f'{uid}/{entry_datetime}/{opt.module_id}-{datetime.now().strftime("%Y%m%d%H%M%S")}.mp4')
            coro = capture_video_and_upload(path, delay, timeout)
        else:
            raise ValueError(f"Unknown 'mode': {mode}. should be either 'image' or 'video'.")
    except KeyError as e:
        return error_response(e, f'A required key {e} is missing.')
    except Exception as e:
        return error_response(e)

    # Run tasks asyncly
    asyncio.create_task(coro)

    return web.json_response({'uri': urljoin(opt.upload_root, str(path))})


@routes.get('/v1/camera/settings')
async def handle_get_camera_settings(request: web.Request):
    width, height = cam.resolution
    return web.json_response({
        'width': width,
        'height': height,
        'framerate': float(cam.framerate),
        'rotation': cam.rotation,
        'exposure_mode': cam.exposure_mode,
    })


@routes.put('/v1/camera/settings')
@assert_camera_idle
async def handle_put_camera_settings(request: web.Request):
    """Update camera settings"""
    # Back up original settings
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


@routes.get('/v1/session')
async def handle_get_session(request: web.Request):
    response = web.Response()
    try:
        cmd = request.query['cmd']
        if cmd == 'enter':
            uid = int(request.query['uid'])
            entry_datetime = request.query['entry_datetime']
            assert entry_datetime.isdigit(), "'entry_datetime' should be the form of 'YYYYmmddHHMMSS'"
            capture_interval = float(request.query.get('capture_interval', opt.capture_interval))

            session = session_manager.create(uid, entry_datetime)
            session.start(capture_interval, 'jpeg', 'h264', level='4.2', bitrate=opt.bitrate, quality=opt.quality)
            response = web.json_response({'session': str(session)}, status=201)
        elif cmd == 'exit':
            session = session_manager.session
            items = session.stop()
            # Upload items
            upload_path = Path(f'{session.uid}/{session.sid}/')
            asyncio.create_task(uploader.upload(upload_path, items))
            response = web.json_response({'session': str(session), 'uri': urljoin(opt.upload_root, str(upload_path))})
        elif cmd == 'interrupt':
            session = session_manager.session
            session_manager.destroy()
            response = web.json_response({'session': str(session)})
        else:
            raise ValueError(f"Unknown cmd={cmd}. should be one of [enter, exit, interrupt].")
    except KeyError as e:
        return error_response(e, f'A required parameter {e} is missing.')
    except PiCameraAlreadyRecording as e:
        session_manager.destroy_silently()
        return error_response(e, 'The camera is busy.', 429)
    except SessionAlreadyExists as e:
        return error_response(e, 'Running session already exists.', code=409)
    except SessionNotExists as e:
        return error_response(e, 'No running session.', code=404)
    except Exception as e:
        return error_response(e)

    return response


app = web.Application()
app.add_routes(routes)

web.run_app(app, port=opt.request_port)
