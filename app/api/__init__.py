# coding:utf-8
from flask import Flask
import os


api = Flask(__name__, static_url_path='')

__all__ = ['login']

from . import login, list
