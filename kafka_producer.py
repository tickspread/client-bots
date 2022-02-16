#!/usr/bin/python
# -*- coding: utf-8 -*-
# host = '10.60.4.20'
import json
from confluent_kafka import Producer, Consumer

# host = 'localhost'
host = '10.60.4.20'

producer = Producer({'bootstrap.servers': '%s:9093' % host})

topic = 'user_data'
key = 'transfer_balance'


messages = [
    {
        "event": "transfer_balance",
        "user_group_id": 0,
        "user_id_from": 15,
        "user_id_to": 37,
        "asset": "USDC",
        "balance_amount": 27880000,
        "asset_precision": 6
    },
    # {
    #     "event": "transfer_balance",
    #     "user_group_id": 0,
    #     "user_id_from": 11,
    #     "user_id_to": 65524,
    #     "asset": "USDC",
    #     "balance_amount": 200000000,
    #     "asset_precision": 6
    # },
    # {
    #     "event": "transfer_balance",
    #     "user_group_id": 0,
    #     "user_id_from": 11,
    #     "user_id_to": 13,
    #     "asset": "USDC",
    #     "balance_amount": 10000000,
    #     "asset_precision": 6
    # }
]

for value in messages:
    producer.produce(topic, json.dumps(value), key)
producer.flush()









"""
PAST MESSAGES
"""


messages = [
{"address":"tb1q3r8mn65e6uxyydwfrln2p20e2td3fkajaxvjye","amount":7100000000,"asset":"testBTCe12","event":"withdraw_request","inserted_at":"2021-06-21T22:26:01.793226","status":"confirmed","updated_at":"2021-06-21T22:26:01.793226","user_group_id":0,"user_id":62,"withdraw_id":16}
]

messages = [
{
    "event": "withdraw_aborted",
    "withdraw_id": 11,
    "user_group_id": 0,
    "user_id": 403,
    "amount": 100000000
},
{
    "event": "withdraw_aborted",
    "withdraw_id": 12,
    "user_group_id": 0,
    "user_id": 403,
    "amount": 100000000
}]

messages = [
{"address":"tb1q84902s0ypnz0h9p8u38a7kkqg07nrj8kv0vd2r","amount":100000000,"asset":"testBTCe12","event":"withdraw_request","inserted_at":"2021-06-21T22:26:01.793226","status":"confirmed","updated_at":"2021-06-21T22:26:01.793226","user_group_id":0,"user_id":403,"withdraw_id":11},
{"address":"tb1q84902s0ypnz0h9p8u38a7kkqg07nrj8kv0vd2r","amount":100000000,"asset":"testBTCe12","event":"withdraw_request","inserted_at":"2021-06-21T22:51:36.050440","status":"confirmed","updated_at":"2021-06-21T22:51:36.050440","user_group_id":0,"user_id":403,"withdraw_id":12},
{"address":"mkhayN3Dk7B6NHrPqvHamFKEE98H5xiQwY","amount":1000000000,"asset":"testBTCe12","event":"withdraw_request","inserted_at":"2021-06-24T18:41:24.736879","status":"confirmed","updated_at":"2021-06-24T18:41:24.736879","user_group_id":0,"user_id":422,"withdraw_id":13},
{"address":"tb1q79lug8nyz0s7he3v8ps2ch8r50dn09g66g7pdr","amount":10000000000,"asset":"testBTCe12","event":"withdraw_request","inserted_at":"2021-06-25T09:57:18.637112","status":"confirmed","updated_at":"2021-06-25T09:57:18.637112","user_group_id":0,"user_id":411,"withdraw_id":14}
]

messages = [{
    'event': 'create_account',
    'id': 395,
    'users': [{
        'account_id': 395,
        'authentication_methods': [{
            'index': 0,
            'key': 'bonus@tickspread.com',
            'status': 'pending',
            'type': 'email_pass',
            'user_id': 65524,
            'value': '$2b$12$pgBq00u7REOjoFiPDaYtoujdEQQbNgc1rfXl7bdqrieS.Q2fj2V8a'
                ,
            }],
        'id': 65524,
        'role': 'admin',
        'user_group_id': 0,
        }],
    'verify': {
        'email': 'bonus@tickspread.com',
        'message': 'verify_email',
        'path': '/confirm_registration',
        'token': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJjbGFpbSI6ImVtYWlsIiwiZW1haWwiOiJib251c0B0aWNrc3ByZWFkLmNvbSJ9.qmCyhmDwSukJbPF7HH6tPmTgdKcagawTqUzFLXaF0kw'
            ,
        },
    }, {
    'event': 'create_account',
    'id': 396,
    'users': [{
        'account_id': 396,
        'authentication_methods': [{
            'index': 0,
            'key': 'error@tickspread.com',
            'status': 'pending',
            'type': 'email_pass',
            'user_id': 65523,
            'value': '$2b$12$A57KIeLQfsbw8nCBf.ykkO3EJXgO2b03KobWTCwdkseNgsMYTIv72'
                ,
            }],
        'id': 65523,
        'role': 'admin',
        'user_group_id': 0,
        }],
    'verify': {
        'email': 'error@tickspread.com',
        'message': 'verify_email',
        'path': '/confirm_registration',
        'token': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJjbGFpbSI6ImVtYWlsIiwiZW1haWwiOiJlcnJvckB0aWNrc3ByZWFkLmNvbSJ9.yC8giaPI0Ju-gNqZkMzwaX5QSSDcN45RhfboMKzHStE'
            ,
        },
    }, {
    'event': 'create_account',
    'id': 397,
    'users': [{
        'account_id': 397,
        'authentication_methods': [{
            'index': 0,
            'key': 'fee@tickspread.com',
            'status': 'pending',
            'type': 'email_pass',
            'user_id': 65522,
            'value': '$2b$12$poapLiJPkbocbttbdyAeGO4vBoUXqVFr3qbbgDZ6hmBmqi/L21pH.'
                ,
            }],
        'id': 65522,
        'role': 'admin',
        'user_group_id': 0,
        }],
    'verify': {
        'email': 'fee@tickspread.com',
        'message': 'verify_email',
        'path': '/confirm_registration',
        'token': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJjbGFpbSI6ImVtYWlsIiwiZW1haWwiOiJmZWVAdGlja3NwcmVhZC5jb20ifQ.yyXMFuoBrV8gcX32sa-NAQy5zyZrf7FJYZvDjxVBxL4'
            ,
        },
    }, {
    'event': 'create_account',
    'id': 398,
    'users': [{
        'account_id': 398,
        'authentication_methods': [{
            'index': 0,
            'key': 'short@tickspread.com',
            'status': 'pending',
            'type': 'email_pass',
            'user_id': 65521,
            'value': '$2b$12$wkRBkKWB1c/dJispUzO3Z.Os2AkSVwA.wUDKckDho7ng19qcxzwxi'
                ,
            }],
        'id': 65521,
        'role': 'admin',
        'user_group_id': 0,
        }],
    'verify': {
        'email': 'short@tickspread.com',
        'message': 'verify_email',
        'path': '/confirm_registration',
        'token': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJjbGFpbSI6ImVtYWlsIiwiZW1haWwiOiJzaG9ydEB0aWNrc3ByZWFkLmNvbSJ9.AKCtxftZNazmpkOXwOFpcrNyV0St2b1wUxeqfN_Iu60'
            ,
        },
    }, {
    'event': 'create_account',
    'id': 399,
    'users': [{
        'account_id': 399,
        'authentication_methods': [{
            'index': 0,
            'key': 'long@tickspread.com',
            'status': 'pending',
            'type': 'email_pass',
            'user_id': 65520,
            'value': '$2b$12$cjgwflRk430JMdhrwi5jgekni4bkOC2FtZSQc/R.BbNxsDKRZjYGq'
                ,
            }],
        'id': 65520,
        'role': 'admin',
        'user_group_id': 0,
        }],
    'verify': {
        'email': 'long@tickspread.com',
        'message': 'verify_email',
        'path': '/confirm_registration',
        'token': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJjbGFpbSI6ImVtYWlsIiwiZW1haWwiOiJsb25nQHRpY2tzcHJlYWQuY29tIn0.kXmIa4zddzXtZd50IBjZAa13zhPEcKvUoFk8hLR9vHw'
            ,
        },
    }]