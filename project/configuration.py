#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------
# Copyright 2023 for Fosun. All Rights Reserved.
# -----------------------------------------------

import click
from pathlib import Path


RootPath = Path(__file__).parent.parent


Defaults = {
    'Information': {
        'ProjectName': 'quant',
        'PublicHost': '0.0.0.0',
        'LocalHost': '0.0.0.0',
        'AlgorithmsPort': 15001,
        'SchedulersPort': 16001,
        'Security': False,
        'Mode': 'development',
    },
    'Callbacks': {
        'Data': 'http://0.0.0.0:15001/mock/callbacks/quant-data',
        'Results': 'http://0.0.0.0:15001/mock/callbacks/quant-results',
    },
    'RabbitMQ': {
        'URL': 'amqp://guest:guest@0.0.0.0:5672//',
        'AlgorithmQueue': 'quant-algorithm',
        'CallbackQueue': 'quant-callback',
    },
    'MinIO': {
        'Endpoint': '0.0.0.0:9089',
        'AccessKey': 'guest',
        'SecretKey': 'guestguest',
        'Bucket': 'quant',
    },
    'Paths': {
        'ProjectPath': RootPath,
        'AlgorithmsPath': RootPath / 'algorithms',
        'ApiPath': RootPath / 'api',
        'AxonPath': RootPath / 'axon',
        'DataPath': RootPath / 'data',
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
@click.option('--ProjectName', 'ProjectName', default=Defaults['Information']['ProjectName'])
@click.option('--PublicHost', 'PublicHost', default=Defaults['Information']['PublicHost'])
@click.option('--LocalHost', 'LocalHost', default=Defaults['Information']['LocalHost'])
@click.option('--AlgorithmsPort', 'AlgorithmsPort', default=Defaults['Information']['AlgorithmsPort'])
@click.option('--SchedulersPort', 'SchedulersPort', default=Defaults['Information']['SchedulersPort'])
@click.option('--Security', 'Security', default=Defaults['Information']['Security'])
@click.option('--Mode', 'Mode', default=Defaults['Information']['Mode'])
@click.pass_context
def information(ctx, **kwargs):
    ctx.meta['Information'].update(
        {
            'ProjectName': kwargs['ProjectName'],
            'PublicHost': kwargs['PublicHost'],
            'LocalHost': kwargs['LocalHost'],
            'AlgorithmsPort': kwargs['AlgorithmsPort'],
            'SchedulersPort': kwargs['SchedulersPort'],
            'Workers': kwargs['Workers'],
            'Security': kwargs['Security'],
            'Mode': kwargs['Mode'],
        }
    )


@group.command('Callbacks')
@click.option('--Data', 'Data', default=Defaults['Callbacks']['Data'])
@click.option('--Results', 'Results', default=Defaults['Callbacks']['Results'])
@click.pass_context
def information_config(ctx, **kwargs):
    ctx.meta['Callbacks'].update(
        {
            'Data': kwargs['Data'],
            'Results': kwargs['Results'],
        }
    )


@group.command('RabbitMQ')
@click.option('--URL', 'URL', default=Defaults['RabbitMQ']['URL'])
@click.option('--AlgorithmQueue', 'AlgorithmQueue', default=Defaults['RabbitMQ']['AlgorithmQueue'])
@click.option('--CallbackQueue', 'CallbackQueue', default=Defaults['RabbitMQ']['CallbackQueue'])
@click.pass_context
def rabbitmq(ctx, **kwargs):
    ctx.meta['RabbitMQ'].update(
        {
            'URL': kwargs['URL'],
            'AlgorithmQueue': kwargs['AlgorithmQueue'],
            'CallbackQueue': kwargs['CallbackQueue'],
        }
    )


@group.command('MinIO')
@click.option('--Bucket', 'Bucket', default=Defaults['MinIO']['Bucket'])
@click.option('--Endpoint', 'Endpoint', default=Defaults['MinIO']['Endpoint'])
@click.option('--AccessKey', 'AccessKey', default=Defaults['MinIO']['AccessKey'])
@click.option('--SecretKey', 'SecretKey', default=Defaults['MinIO']['SecretKey'])
@click.pass_context
def rabbitmq_config(ctx, **kwargs):
    ctx.meta['MinIO'].update(
        {
            'Endpoint': kwargs['Endpoint'],
            'AccessKey': kwargs['AccessKey'],
            'SecretKey': kwargs['SecretKey'],
            'Bucket': kwargs['Bucket'],
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
