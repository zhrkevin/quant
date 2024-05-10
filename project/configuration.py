#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import json
import click
from datetime import datetime
from pathlib import Path, PosixPath


ProjectPath = Path(__file__).parent.parent
Version = datetime.now().strftime('%Y-%m-%d %H:%M:%S')


Defaults = {
    'Information': {
        'Project': 'quant',
        'Host': '0.0.0.0',
        'AlgorithmPort': 10000,
        'SchedulerPort': 20000,
        'Security': False,
        'Version': Version,
        'Mode': 'development',
    },
    'MinIO': {
        'Endpoint': 'minio-api.jingzhi-sh.com:3456',
        'AccessKey': 'algorithm-user',
        'SecretKey': '1cciUuToLVqi9tja',
        'Bucket': 'quant',
    },
    'RabbitMQ': {
        'Endpoint': 'rabbitmq.jingzhi-sh.com:5678/algorithm',
        'Username': 'algorithm-user',
        'Password': 'HBbB4NUnQ8yTWhHh',
        'CallbackQueue': 'quant-callback',
    },
    'Callbacks': {
        'Mock': 'http://0.0.0.0:10000/quant/callback/mock',
    },
    'Paths': {
        'ProjectPath': ProjectPath,
        'AlgorithmsPath': ProjectPath / 'algorithms',
        'ApiPath': ProjectPath / 'api',
        'AxonPath': ProjectPath / 'axon',
        'DataPath': ProjectPath / 'data',
    },
}


@click.group(chain=True, invoke_without_command=True)
@click.pass_context
def group(ctx):
    ctx.meta.update(Defaults)


@group.result_callback()
@click.pass_context
def callback(ctx, processors):
    return ctx.meta


@group.command('Information')
@click.option('--Project', 'Project', default=Defaults['Information']['Project'])
@click.option('--Host', 'Host', default=Defaults['Information']['Host'])
@click.option('--AlgorithmPort', 'AlgorithmPort', default=Defaults['Information']['AlgorithmPort'])
@click.option('--SchedulerPort', 'SchedulerPort', default=Defaults['Information']['SchedulerPort'])
@click.option('--Security', 'Security', default=Defaults['Information']['Security'])
@click.option('--Version', 'Version', default=Defaults['Information']['Version'])
@click.option('--Mode', 'Mode', default=Defaults['Information']['Mode'])
@click.pass_context
def information(ctx, **kwargs):
    ctx.meta['Information'] |= {
        'Project': kwargs['Project'],
        'Host': kwargs['Host'],
        'AlgorithmPort': kwargs['AlgorithmPort'],
        'SchedulerPort': kwargs['SchedulerPort'],
        'Security': kwargs['Security'],
        'Version': kwargs['Version'],
        'Mode': kwargs['Mode'],
    }


@group.command('RabbitMQ')
@click.option('--Endpoint', 'Endpoint', default=Defaults['RabbitMQ']['Endpoint'])
@click.option('--Username', 'Username', default=Defaults['RabbitMQ']['Username'])
@click.option('--Password', 'Password', default=Defaults['RabbitMQ']['Password'])
@click.option('--CallbackQueue', 'CallbackQueue', default=Defaults['RabbitMQ']['CallbackQueue'])
@click.pass_context
def rabbitmq(ctx, **kwargs):
    ctx.meta['RabbitMQ'].update(
        {
            'Endpoint': kwargs['Endpoint'],
            'Username': kwargs['Username'],
            'Password': kwargs['Password'],
            'CallbackQueue': kwargs['CallbackQueue'],
        }
    )


@group.command('MinIO')
@click.option('--Endpoint', 'Endpoint', default=Defaults['MinIO']['Endpoint'])
@click.option('--AccessKey', 'AccessKey', default=Defaults['MinIO']['AccessKey'])
@click.option('--SecretKey', 'SecretKey', default=Defaults['MinIO']['SecretKey'])
@click.option('--Bucket', 'Bucket', default=Defaults['MinIO']['Bucket'])
@click.pass_context
def minio(ctx, **kwargs):
    ctx.meta['MinIO'].update(
        {
            'Endpoint': kwargs['Endpoint'],
            'AccessKey': kwargs['AccessKey'],
            'SecretKey': kwargs['SecretKey'],
            'Bucket': kwargs['Bucket'],
        }
    )


@group.command('Callbacks')
@click.option('--Mock', 'Mock', default=Defaults['Callbacks']['Mock'])
@click.pass_context
def callbacks(ctx, **kwargs):
    ctx.meta['Callbacks'].update(
        {
            'Mock': kwargs['Mock'],
        }
    )


@group.command('Paths')
@click.option('--ProjectPath', 'ProjectPath', default=Defaults['Paths']['ProjectPath'])
@click.option('--AlgorithmsPath', 'AlgorithmsPath', default=Defaults['Paths']['AlgorithmsPath'])
@click.option('--ApiPath', 'ApiPath', default=Defaults['Paths']['ApiPath'])
@click.option('--AxonPath', 'AxonPath', default=Defaults['Paths']['AxonPath'])
@click.option('--DataPath', 'DataPath', default=Defaults['Paths']['DataPath'])
@click.pass_context
def paths_config(ctx, **kwargs):
    ctx.meta['Paths'].update(
        {
            'ProjectPath': kwargs['ProjectPath'],
            'AlgorithmsPath': kwargs['AlgorithmsPath'],
            'ApiPath': kwargs['ApiPath'],
            'AxonPath': kwargs['AxonPath'],
            'DataPath': kwargs['DataPath'],
        }
    )


Config = group.main(standalone_mode=False)


if __name__ == '__main__':
    class Encoding(json.JSONEncoder):
        def default(self, data):
            if isinstance(data, PosixPath):
                return str(data)

    j = json.dumps(Config, cls=Encoding, indent=4, ensure_ascii=False)
    print(j)
