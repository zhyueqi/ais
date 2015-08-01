# coding:utf-8
from flask import request, jsonify
from . import api


# @api.route('/login', methods=['POST'])
# def login():
#     username = request.form['username']
#     password = request.form['password']
#     if verify(username, password):
#         return jsonify({
#             'code': '001',
#             'message': 'OK'
#         })
#     return jsonify({
#         'code': '002',
#         'message': 'Failed'
#     })


@api.route('/flask/')
def index():
    return "<html><head><title>shippingData</title></head><body>ShippingData</body></html>"
