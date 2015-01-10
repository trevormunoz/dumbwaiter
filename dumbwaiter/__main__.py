#!/usr/bin/env python

import argparse
import dumbwaiter.pipeline as dw

def main(args=None):
     """The main routine"""

     parser = argparse.ArgumentParser(
         description="Extract, transform, and load NYPL menu data.")

     parser.add_argument('path',
                         help='path to data file(s)')
     parser.add_argument('-s',
                         '--server',
                         nargs='?',
                         default='localhost',
                         help='hostname of elasticsearch server')
     parser.add_argument('-p',
                         '--port',
                         nargs='?',
                         default='9200',
                         help="port to use on elasticsearch server")

     args = parser.parse_args()
     dw.load(args.path, host=args.server, port=args.port)

if __name__ == '__main__':
     main()