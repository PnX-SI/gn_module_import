#!/usr/bin/env python
# -*- coding: utf-8 -*-
 

import logging
 
from logging.handlers import RotatingFileHandler
 
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s :: %(levelname)s :: %(message)s')

"""
# logs to file
file_handler = RotatingFileHandler('../external_modules/import/var/activity.log', 'a', 10000000, 1)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
"""