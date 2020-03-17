#!/usr/bin/python2.7
import tempfile
import subprocess
import shutil
import sys
import base64


class FFMpegOutputWriter:
    def __init__(self):
        self.img_id = 0
        self.tempd = tempfile.mkdtemp(prefix='out_video_')

    def cleanup(self):
        shutil.rmtree(self.tempd)

    def __write_image(self, content):
        out = open('%s/img_%04d.jpg' % (self.tempd, self.img_id), 'w')
        out.write(content)
        out.close()
        self.img_id += 1

    def add_file(self, f):
        for line in f:
            content = base64.b64decode(line.rstrip())
            self.__write_image(content)

    def encode(self, out_path):
        cmd_encode = ['ffmpeg', '-hide_banner', '-framerate', '24', 
                      '-i', '%s/img_%%04d.jpg' % self.tempd, '-an', '-r', '24', '-y', out_path]
        return subprocess.call(cmd_encode)


def convert2video():
    if len(sys.argv) < 2:
        print 'Usage: %s mapper-output-files' % sys.argv[0]
        sys.exit(64)

    writer = FFMpegOutputWriter()
    try:
        for mapfile in sorted(sys.argv[1:]):
            with open(mapfile) as f:
                writer.add_file(f)

        rc = writer.encode('out.mp4')
        return rc
    finally:
        writer.cleanup()

if __name__ == '__main__':
    sys.exit(convert2video())
