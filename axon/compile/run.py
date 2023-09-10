#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------
# Copyright 2023 for Fosun. All Rights Reserved.
# -----------------------------------------------

import os
import sys
import json
import pathspec
from setuptools import setup
from termcolor import cprint
from Cython.Build import cythonize, build_ext

from project.configuration import Config


ProjectPath = Config['Paths']['ProjectPath']
AlgorithmsPath = Config['Paths']['AlgorithmsPath']
BackupPath = Config['Paths']['ProjectPath'] / 'backup'
BuildPath = Config['Paths']['ProjectPath'] / 'build'


Ignores = [
    '__main__.py',
    '__init__.py',
    '*.dict',
    'essentials/',
]
Exclude = pathspec.PathSpec.from_lines('gitwildmatch', Ignores).match_file


class Compile(object):

    operation = sys.argv[1] if len(sys.argv) > 1 else 'check'
    os.system(f"find {ProjectPath} -name '__pycache__' -o -name '.DS_Store' | xargs rm -rfv")

    def __init__(self):
        if self.operation == 'check':
            self.check()
        elif self.operation == 'compile':
            self.build()
            self.install()
            self.clean()
        elif self.operation == 'build':
            self.build()
        elif self.operation == 'install':
            self.install()
        elif self.operation == 'clean':
            self.clean()
        elif self.operation == 'rollback':
            self.rollback()
        else:
            raise IOError("使用方式：python run.py [选择: compile | build | install | clean | rollback ]")

    @staticmethod
    def build():
        os.system(f"rm -rfv {BackupPath}/* && mkdir -p {BackupPath} && cp -rfv {AlgorithmsPath} {BackupPath}")

        extensions = []
        for rootpath, dirnames, filenames in os.walk(AlgorithmsPath):
            for filename in filenames:
                if not Exclude(f"{rootpath}/{filename}"):
                    extensions.append(f"{rootpath}/{filename}")

        modules = cythonize(
            extensions,
            nthreads=20,
            compiler_directives={'always_allow_keywords': True, 'language_level': 3}
        )
        setup(
            ext_modules=modules,
            cmdclass={'build_ext': build_ext},
        )
        cprint(f"编译已完成。", color='red')

    @staticmethod
    def install():
        os.system(f"cp -rfv {BuildPath}/lib*/algorithms {ProjectPath}")
        os.system(f"find {AlgorithmsPath}/* -name '*.c' -o -name '*.py' | egrep -v '__init__.py' | xargs rm -rfv")
        cprint(f"安装已完成。", color='red')

    @staticmethod
    def rollback():
        os.system(f"rm -rfv {AlgorithmsPath}/*")
        os.system(f"cp -rfv {BackupPath}/algorithms {ProjectPath}")
        cprint(f"回滚已完成。", color='red')

    @staticmethod
    def clean():
        os.system(f"rm -rfv {BackupPath} {BuildPath}")
        cprint(f"此命令已删除所有源码！", color='red')

    @staticmethod
    def check():
        extensions = []
        for rootpath, dirnames, filenames in os.walk(AlgorithmsPath):
            for filename in filenames:
                if not Exclude(f"{rootpath}/{filename}"):
                    extensions.append(f"{rootpath}/{filename}")

        print(json.dumps(extensions, indent=4, ensure_ascii=False))


if __name__ == '__main__':
    Compile()
