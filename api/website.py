#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import sanic

from project import Config


website_blueprint = sanic.Blueprint(name='WebsiteBlueprint')
website_blueprint.static(uri='/quant/site', name='site', file_or_directory=Config['Paths']['WebsitePath'] / 'site', index="index.html")
website_blueprint.static(uri='/quant/swagger', name='swagger', file_or_directory=Config['Paths']['WebsitePath'] / 'swagger', index="index.html")


@website_blueprint.route(f"/")
async def root_route_redirect(request):
    return sanic.redirect("/quant/site/index.html")


@website_blueprint.route(f"/quant")
async def main_route_redirect(request):
    return sanic.redirect("/quant/site/index.html")


@website_blueprint.get('/quant/validator/<metrics:path>')
async def swagger_validator(request, metrics):
    valid = Config['Paths']['WebsitePath'] / 'swagger' / 'valid.png'
    return await sanic.response.file(valid)
