#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------------------
# Copyright 2024 for Jingzhi & Level. All Rights Reserved.
# ---------------------------------------------------------

import os
import sys
import loguru
import inspect

from project import Config


class StackFrame:

    def __init__(self, extra, **kwargs):
        self.extra = extra
        self.kwargs = kwargs

    def __enter__(self):
        frame = inspect.getframeinfo(inspect.stack()[3][0])
        self.extra |= {
            'line': frame.lineno,
            'function': frame.function,
            'file': os.path.basename(frame.filename),
            **self.kwargs,
        }
        return self

    def __exit__(self, exc_type, exc_value, trace_back):
        return False


class Loguru:

    mode = Config['Information']['Mode']

    # format documents: https://loguru.readthedocs.io/en/stable/api/logger.html#loguru._logger.Logger.add
    format = "<level>[{time:YYYY-MM-DD:HH:mm:ss}] {message}</level>"

    def __init__(self):
        loguru.logger.remove()
        loguru.logger.add(sys.stdout, colorize=True, enqueue=False, format=self.format)
        loguru.logger.add(Config['Paths']['DataPath'] / 'output' / f'report.log', rotation='2 MB', enqueue=False, format=self.format, mode='w')

        self.extra = {'taskid': 'DefaultTaskID'}
        self.logger = loguru.logger.bind(name='DefaultTaskID')

    def set(self, symbol=None):
        self.extra |= {'symbol': symbol}
        self.logger = loguru.logger.bind(name=symbol)

    def log(self, information, code, color, level, **kwargs):
        loguru.logger.level(name=level, color=color)
        with StackFrame(extra=self.extra, code=code):
            with self.logger.contextualize(**self.extra):
                self.logger.log(level, information)
        message = {'code': code, 'information': information, **kwargs}
        return message

    def debug(self, information=None, code=100, color='<fg #0055FF>', **kwargs):
        return self.log(information, code, color, level='DEBUG', **kwargs)

    def info(self, information=None, code=200, color='<n>', **kwargs):
        return self.log(information, code, color, level='INFO', **kwargs)

    def success(self, information=None, code=200, color='<fg #00AA33>', **kwargs):
        return self.log(information, code, color, level='SUCCESS', **kwargs)

    def warning(self, information=None, code=300, color='<fg #FFAA00>', **kwargs):
        return self.log(information, code, color, level='WARNING', **kwargs)

    def error(self, information=None, code=500, color='<fg #FF3333>', **kwargs):
        return self.log(information, code, color, level='ERROR', **kwargs)

    def critical(self, information=None, code=500, color='<fg #FF3333><v>', **kwargs):
        return self.log(information, code, color, level='CRITICAL', **kwargs)


Logger = Loguru()


if __name__ == '__main__':
    Logger.set(symbol='aaa')

    def llll():
        Logger.info(code=100, information='zzz')
        message = Logger.info(code=200, information='yyy')
        print(message)


    def mmmm():
        Logger.info(code=300, information='xxx')
        message = Logger.info(code=400, information='www', extra='uuu')
        print(message)
        Logger.debug(information='www', extra='uuu')
        Logger.info(information='www', extra='uuu')
        Logger.success(information='www', extra='uuu')
        Logger.warning(information='www', extra='uuu')
        Logger.error(information='www', extra='uuu')
        Logger.critical(information='www', extra='uuu')

    llll()
    mmmm()

    print(1)
