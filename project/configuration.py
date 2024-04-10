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
        'Project': 'quant',
        'Host': '0.0.0.0',
        'AlgorithmPort': 10000,
        'SchedulerPort': 20000,
        'Security': False,
        'Mode': 'development',
    },
    'RabbitMQ': {
        'Endpoint': 'rabbitmq.jingzhi-sh.com:5678/algorithm',
        'Username': 'algorithm-user',
        'Password': 'HBbB4NUnQ8yTWhHh',
    },
    'Queues': {
        'Data': 'non-standard-aps-data',
        'Algorithm': 'non-standard-aps-algorithm',
    },
    'MinIO': {
        'Endpoint': 'minio-api.jingzhi-sh.com:3456',
        'AccessKey': 'algorithm-user',
        'SecretKey': '1cciUuToLVqi9tja',
        'Bucket': 'non-standard-aps',
    },
    'Callbacks': {
        'Mock': 'http://0.0.0.0:10000/callback/mock',
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
@click.option('--Project', 'Project', default=Defaults['Information']['Project'])
@click.option('--Host', 'Host', default=Defaults['Information']['Host'])
@click.option('--AlgorithmPort', 'AlgorithmPort', default=Defaults['Information']['AlgorithmPort'])
@click.option('--SchedulerPort', 'SchedulerPort', default=Defaults['Information']['SchedulerPort'])
@click.option('--Security', 'Security', default=Defaults['Information']['Security'])
@click.option('--Mode', 'Mode', default=Defaults['Information']['Mode'])
@click.pass_context
def information(ctx, **kwargs):
    ctx.meta['Information'].update(
        {
            'Project': kwargs['Project'],
            'Host': kwargs['Host'],
            'AlgorithmPort': kwargs['AlgorithmPort'],
            'SchedulerPort': kwargs['SchedulerPort'],
            'Security': kwargs['Security'],
            'Mode': kwargs['Mode'],
        }
    )


@group.command('RabbitMQ')
@click.option('--Endpoint', 'Endpoint', default=Defaults['RabbitMQ']['Endpoint'])
@click.option('--Username', 'Username', default=Defaults['RabbitMQ']['Username'])
@click.option('--Password', 'Password', default=Defaults['RabbitMQ']['Password'])
@click.pass_context
def rabbitmq(ctx, **kwargs):
    ctx.meta['RabbitMQ'].update(
        {
            'Endpoint': kwargs['Endpoint'],
            'Username': kwargs['Username'],
            'Password': kwargs['Password'],
        }
    )


@group.command('Queues')
@click.option('--Data', 'Data', default=Defaults['Queues']['Data'])
@click.option('--Algorithm', 'Algorithm', default=Defaults['Queues']['Algorithm'])
@click.pass_context
def queues(ctx, **kwargs):
    ctx.meta['Queues'].update(
        {
            'Data': kwargs['Data'],
            'Algorithm': kwargs['Algorithm'],
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
