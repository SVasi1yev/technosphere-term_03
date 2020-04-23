#!/usr/bin/env python

import document_pb2
import struct
import argparse
import gzip
import base64
import sys

class DocumentStreamReader:
    def __init__(self, paths):
        self.paths = paths

    def open_single(self, path):
        return gzip.open(path, 'rb') if path.endswith('.gz') else open(path, 'rb')

    def __iter__(self):
        for path in self.paths:
            with self.open_single(path) as stream:
                while True:
                    sb = stream.read(4)
                    if sb == '':
                        break

                    size = struct.unpack('i', sb)[0]
                    msg = stream.read(size)
                    doc = document_pb2.document()
                    doc.ParseFromString(msg)
                    yield doc


def parse_command_line():
    parser = argparse.ArgumentParser(description='document converter')
    parser.add_argument('files', nargs='+', help='Input files (.gz or plain) to process')
    return parser.parse_args()


def main():
    opts = parse_command_line()
    reader = DocumentStreamReader(opts.files)

    for doc in reader:
        #print "%s\t%d bytes" % (doc.url, len(doc.text) if doc.HasField('text') else -1)
        if doc.HasField('text'):
            print "%s\t%s" % (doc.url, base64.b64encode(doc.text.encode('utf-8')))


if __name__ == '__main__':
    main()
