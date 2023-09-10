#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------
# Copyright 2023 for Fosun. All Rights Reserved.
# -----------------------------------------------

import sanic

from project.configuration import Config


website_blueprint = sanic.Blueprint(name='WebsiteBlueprint')
website_blueprint.static(uri='/', name='root', file_or_directory=Config['Paths']['AxonPath'] / 'homepage')
website_blueprint.static(uri='/swagger', name='swagger', file_or_directory=Config['Paths']['AxonPath'] / 'swagger')


@website_blueprint.route("/")
async def main_route_redirect(request):
    return sanic.redirect("/index.html")


@website_blueprint.get('/validator/<metrics:path>')
async def swagger_validator(request, metrics):
    valid = Config['Paths']['AxonPath'] / 'swagger' / 'valid.png'
    return await sanic.response.file(valid)
