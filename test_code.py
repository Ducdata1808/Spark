import logging
import time


def vn_time(*args):
    return time.localtime(time.time() + 7 * 3600)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='error.log',
    filemode='a'
)
logging.Formatter.converter = vn_time
logger = logging.getLogger(__name__)
logger.info("this is time in Vietnam")