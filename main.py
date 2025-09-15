from datetime import datetime, timezone
import logging
import time
import os
import argparse

from waggle.plugin import Plugin
from waggle.data.vision import Camera
from croniter import croniter

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(message)s',
    datefmt='%Y/%m/%d %H:%M:%S')


def capture(plugin, stream, out_dir=""):
    sample_file_name = "sample.jpg"
    
    with Camera(stream) as camera:
        for snapshot in camera.stream():
            sample = snapshot
            break
    
    if out_dir == "":
        sample.save(sample_file_name)
        meta = {"camera": stream}
        plugin.upload_file(sample_file_name, meta=meta)
    else:
        dt = datetime.fromtimestamp(sample.timestamp / 1e9)
        base_dir = os.path.join(out_dir, dt.astimezone(timezone.utc).strftime('%Y/%m/%d/%H'))
        os.makedirs(base_dir, exist_ok=True)
        sample_path = os.path.join(base_dir, dt.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S%z.jpg'))
        sample.save(sample_path)


def main(args):
    logging.info(f'Starting image sampler for {args.stream}')
    
    if args.cronjob == "":
        logging.info('Single capture mode')
        with Plugin() as plugin:
            capture(plugin, args.stream, args.out_dir)
        return 0
    
    if not croniter.is_valid(args.cronjob):
        logging.error(f'Invalid cronjob format: {args.cronjob}')
        return 1
        
    logging.info(f'Cronjob mode: {args.cronjob}')
    now = datetime.now(timezone.utc)
    cron = croniter(args.cronjob, now)
    
    with Plugin() as plugin:
        while True:
            n = cron.get_next(datetime).replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)
            next_in_seconds = (n - now).total_seconds()
            if next_in_seconds > 0:
                logging.info(f'Sleeping for {next_in_seconds} seconds')
                time.sleep(next_in_seconds)
            logging.info('Capturing...')
            capture(plugin, args.stream, args.out_dir)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--stream', required=True, help='RTSP stream URL')
    parser.add_argument('--out-dir', default="", help='Local output directory')
    parser.add_argument('--cronjob', default="", help='Cronjob schedule')
    
    args = parser.parse_args()
    if args.out_dir:
        os.makedirs(args.out_dir, exist_ok=True)
    exit(main(args))