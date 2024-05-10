#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import os
import sys
import json
from pathlib import Path
from pathspec import PathSpec
from setuptools import setup
from Cython.Build import cythonize, build_ext


ProjectPath = Path(__file__).parent.parent
AlgorithmsPath = ProjectPath / 'algorithms'
BuildPath = ProjectPath / 'build'
BackupPath = ProjectPath / 'backup'


Ignores = [
    '__main__.py',
    '__init__.py',
]
Exclude = PathSpec.from_lines('gitwildmatch', Ignores).match_file


class Compile(object):

    operation = sys.argv[1] if len(sys.argv) > 1 else 'check'
    os.system(f"find {ProjectPath} -name '__pycache__' -o -name '.DS_Store' | xargs rm -rfv")

    def __init__(self, debug=False):
        if self.operation == 'build':
            self.build(debug)
        elif self.operation == 'rollback':
            self.rollback()
        elif self.operation == 'check':
            self.check()
        else:
            raise IOError("使用方式：python setup.py [选择: build | rollback | check ]")

    @staticmethod
    def build(debug):
        os.system(f"mkdir -p {BackupPath} && cp -rfv {AlgorithmsPath} {BackupPath}")

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
            py_modules=[],
            ext_modules=modules,
            cmdclass={'build_ext': build_ext},
        )
        print(f"文件编译已完成。")

        os.system(f"cp -rfv {BuildPath}/lib*/algorithms {ProjectPath}")
        os.system(f"find {AlgorithmsPath}/* -name '*.c' -o -name '*.py' | egrep -v '__init__.py' | xargs rm -rfv")
        os.system(f"rm -rfv {BackupPath} {BuildPath}") if not debug else None
        print(f"安装清理已完成。")

    @staticmethod
    def rollback():
        os.system(f"rm -rfv {AlgorithmsPath}/*")
        os.system(f"cp -rfv {BackupPath}/algorithms {ProjectPath}")
        print(f"回滚已完成。")

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
