from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
import logging

from aiohttp import web
from picamera import PiCamera

from api_v1 import api_v1
from session import SessionManager
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


async def initialize(app: web.Application):
    # Initialize Pi camera hardware
    cam = PiCamera()
    cam.resolution = opt.width, opt.height
    cam.framerate = opt.framerate
    cam.rotation = opt.rotation
    cam.exposure_mode = opt.exposure_mode
    app['camera'] = cam
    # One and only session manager
    app['session_manager'] = SessionManager()
    # Item uploader
    app['uploader'] = ItemUploader(opt.upload_endpoint, opt.module_id, opt.token)
    # Settings
    app['bitrate'] = opt.bitrate
    app['quality'] = opt.quality
    app['delay'] = opt.delay
    app['timeout'] = opt.timeout
    app['capture_interval'] = opt.capture_interval
    app['upload_endpoint'] = opt.upload_endpoint
    app['upload_root'] = opt.upload_root
    app['token'] = opt.token
    app['module_id'] = opt.module_id


async def cleanup(app):
    # TODO: Dispose uploader
    app['session_manager'].destroy_silently()
    app['camera'].close()


app = web.Application()
app.on_startup.append(initialize)
app.on_cleanup.append(cleanup)
app.add_subapp('/v1/', api_v1)

web.run_app(app, port=opt.request_port)
