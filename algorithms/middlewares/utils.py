#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

from json import JSONEncoder
from datetime import datetime
from pathlib import PosixPath


class Encoding(JSONEncoder):

    def default(self, data):
        if isinstance(data, bytes):
            return data.decode('utf-8')
        elif isinstance(data, datetime):
            return data.strftime("%F %T.%f")
        elif isinstance(data, PosixPath):
            return str(data)
        else:
            raise OSError(f'{data} cannot dump to json')


class TicToc:

    tictime, toctime = None, None

    @classmethod
    def tic(cls):
        cls.toctime = cls.tictime = datetime.now()

    @classmethod
    def toc(cls):
        cls.toctime = datetime.now()
        print(f"\nStarting Time:     {cls.tictime}\r")
        print(f"\rFinishing Time:    {cls.toctime}\r")
        print(f"\rElapsed Time:      {cls.toctime - cls.tictime}\n")
