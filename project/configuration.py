#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import click
from box import Box
from pathlib import Path


ProjectPath = Path(__file__).parent.parent


Defaults = Box(
    {
        'Information': {
            'Host': '0.0.0.0',
            'AlgorithmPort': 10001,
            'SchedulerPort': 20001,
            'Security': False,
            'Mode': 'test',
        },
        'MinIO': {
            'Endpoint': '0.0.0.0:3456',
            'AccessKey': 'algorithm-user',
            'SecretKey': '1cciUuToLVqi9tja',
            'Bucket': 'quant',
        },
        'RabbitMQ': {
            'Endpoint': '0.0.0.0:5678/algorithm',
            'Username': 'algorithm-user',
            'Password': 'HBbB4NUnQ8yTWhHh',
            'CallbackQueue': 'quant',
        },
        'Callbacks': {
            'Mock': 'http://0.0.0.0:10001/quant/callback/mock',
        },
        'Paths': {
            'ProjectPath': ProjectPath,
            'AlgorithmsPath': ProjectPath / 'algorithms',
            'ApiPath': ProjectPath / 'api',
            'DocsPath': ProjectPath / 'docs',
            'DataPath': ProjectPath / 'data',
        },
    }
)

@click.group(chain=True, invoke_without_command=True)
@click.pass_context
def group(ctx):
    ctx.obj = Defaults


@group.result_callback()
@click.pass_context
def callback(ctx, processors):
    return ctx.obj


@group.command('Information')
@click.option('--Host', 'Host', default=Defaults['Information']['Host'])
@click.option('--AlgorithmPort', 'AlgorithmPort', default=Defaults['Information']['AlgorithmPort'])
@click.option('--SchedulerPort', 'SchedulerPort', default=Defaults['Information']['SchedulerPort'])
@click.option('--Security', 'Security', default=Defaults['Information']['Security'])
@click.option('--Mode', 'Mode', default=Defaults['Information']['Mode'])
@click.pass_context
def information(ctx, **kwargs):
    ctx.obj['Information'] |= {
        'Host': kwargs['Host'],
        'AlgorithmPort': kwargs['AlgorithmPort'],
        'SchedulerPort': kwargs['SchedulerPort'],
        'Security': kwargs['Security'],
        'Mode': kwargs['Mode'],
    }


@group.command('MinIO')
@click.option('--Endpoint', 'Endpoint', default=Defaults['MinIO']['Endpoint'])
@click.option('--AccessKey', 'AccessKey', default=Defaults['MinIO']['AccessKey'])
@click.option('--SecretKey', 'SecretKey', default=Defaults['MinIO']['SecretKey'])
@click.option('--Bucket', 'Bucket', default=Defaults['MinIO']['Bucket'])
@click.pass_context
def minio(ctx, **kwargs):
    ctx.obj['MinIO'].update(
        {
            'Endpoint': kwargs['Endpoint'],
            'AccessKey': kwargs['AccessKey'],
            'SecretKey': kwargs['SecretKey'],
            'Bucket': kwargs['Bucket'],
        }
    )


@group.command('RabbitMQ')
@click.option('--Endpoint', 'Endpoint', default=Defaults['RabbitMQ']['Endpoint'])
@click.option('--Username', 'Username', default=Defaults['RabbitMQ']['Username'])
@click.option('--Password', 'Password', default=Defaults['RabbitMQ']['Password'])
@click.option('--CallbackQueue', 'CallbackQueue', default=Defaults['RabbitMQ']['CallbackQueue'])
@click.pass_context
def rabbitmq(ctx, **kwargs):
    ctx.obj['RabbitMQ'].update(
        {
            'Endpoint': kwargs['Endpoint'],
            'Username': kwargs['Username'],
            'Password': kwargs['Password'],
            'CallbackQueue': kwargs['CallbackQueue'],
        }
    )


@group.command('Callbacks')
@click.option('--Mock', 'Mock', default=Defaults['Callbacks']['Mock'])
@click.pass_context
def callbacks(ctx, **kwargs):
    ctx.obj['Callbacks'].update(
        {
            'Mock': kwargs['Mock'],
        }
    )


@group.command('Paths')
@click.option('--ProjectPath', 'ProjectPath', default=Defaults['Paths']['ProjectPath'])
@click.option('--AlgorithmsPath', 'AlgorithmsPath', default=Defaults['Paths']['AlgorithmsPath'])
@click.option('--ApiPath', 'ApiPath', default=Defaults['Paths']['ApiPath'])
@click.option('--DocsPath', 'DocsPath', default=Defaults['Paths']['DocsPath'])
@click.option('--DataPath', 'DataPath', default=Defaults['Paths']['DataPath'])
@click.pass_context
def paths_config(ctx, **kwargs):
    ctx.obj['Paths'].update(
        {
            'ProjectPath': kwargs['ProjectPath'],
            'AlgorithmsPath': kwargs['AlgorithmsPath'],
            'ApiPath': kwargs['ApiPath'],
            'DocsPath': kwargs['DocsPath'],
            'DataPath': kwargs['DataPath'],
        }
    )


Config = group.main(standalone_mode=False)


if __name__ == '__main__':
    print(Config.Paths.DataPath)
