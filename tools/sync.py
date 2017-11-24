#!/bin/env python
#coding=utf-8
import requests
import logging
import signal
import json
import collections
import time
import sys
import MySQLdb
import httplib
import urllib
import os

time.sleep( 20 )

# Setup Mvsd connection
rpc_uri = 'http://%s:%s/rpc' % (os.environ['MVSD_HOST'], os.environ['MVSD_PORT'])

# Database connection
host = os.environ['DB_HOST']
port = int(os.environ['DB_PORT'])
user = os.environ['DB_USER']
passwd = os.environ['DB_PASS']
db_name = os.environ['DB_NAME']

max_try =int(os.environ['MAX_RETRY']) if int(os.environ['MAX_RETRY']) else 10

# Global asset
gloab_asset = {}

null_hash = '0' * 64


def set_up_logging():
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='block_sync.log',
                filemode='a+')
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)

set_up_logging()


class Boolean:
    def __init__(self):
        self.__value = False

    def __call__(self, *args, **kwargs):
        if len(args) > 1:
            raise RuntimeError('undefined action')

        if not args:
            return self.__value

        self.__value = bool(args[0])


class HeaderNotFound(Exception):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


class TryException(Exception):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


class RpcConnectionException(Exception):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


class MongodbConnectionException(Exception):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


class CriticalException(Exception):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


def new_mysql(host, port, user, passwd, db_name):
    conn = MySQLdb.connect(db= db_name, host = host, port = port, user = user, passwd = passwd)
    return conn


def init_table(conn):
    tables = []
    tb_block = '''
    create table if not EXISTS block (
      number bigint primary KEY ,
      hash char(64) not null,
      bits bigint,
      transaction_count INTEGER ,
      mixhash  VARCHAR (128),
      version char(8) ,
      merkle_tree_hash char(64),
      previous_block_hash CHAR (64),
      nonce varchar(128) ,
      time_stamp bigint
    ) DEFAULT charset=utf8;
    '''

    tb_tx = '''
      create table if not EXISTS tx (
        id bigint PRIMARY KEY ,
        block_height bigint REFERENCES block(id),
        hash char(64) not null
      )DEFAULT charset=utf8 ;'''

    tb_address = '''
        create table if not EXISTS address(
            id int PRIMARY KEY ,
            address VARCHAR (64) UNIQUE
        )DEFAULT charset=utf8;
    '''

    tb_output = '''
        create table if not EXISTS tx_output(
          id bigint PRIMARY key,
          hash char(64) NOT NULL ,
          tx_id bigint REFERENCES tx(id),
          output_index bigint not null,
          output_value bigint,
          address_id bigint REFERENCES address(id),
          script varchar(1024),
          asset varchar(64),
          decimal_number varchar(8)
        )DEFAULT charset=ascii;
    '''

    tb_output_fork = '''
            create table if not EXISTS tx_output_fork(
              id bigint PRIMARY key,
              hash char(64) NOT NULL ,
              tx_id bigint,
              output_index bigint not null,
              output_value bigint,
              address_id bigint,
              script varchar(1024),
              asset varchar(64),
              decimal_number varchar(8)
            )DEFAULT charset=ascii;
        '''
    tb_tx_fork = '''
          create table if not EXISTS tx_fork (
            id bigint PRIMARY KEY ,
            block_height bigint,
            hash char(64) not null
          )DEFAULT charset=ascii ;'''

    tb_input_fork = '''
            create table if not EXISTS tx_input_fork(
              id bigint PRIMARY key,
              tx_id bigint,
              belong_tx_id bigint,
              tx_index bigint,
              tx_value bigint not null,
              script varchar(1024),
              address_id bigint,
              asset varchar(64),
              decimal_number varchar(8)
            )DEFAULT charset=ascii;
        '''

    tb_block_fork = '''
        create table if not EXISTS block_fork (
          number bigint primary KEY ,
          hash char(64) not null,
          bits bigint,
          transaction_count INTEGER ,
          mixhash  VARCHAR (128),
          version char(8) ,
          merkle_tree_hash char(64),
          previous_block_hash CHAR (64),
          nonce varchar(128) ,
          time_stamp bigint
        ) DEFAULT charset=ascii;
        '''
    tb_output_asset = '''
        create table if not EXISTS tx_output_asset(
          id bigint PRIMARY key,
          hash char(64) NOT NULL ,
          tx_id bigint REFERENCES tx(id),
          output_index bigint not null,
          output_value bigint,
          address_id bigint REFERENCES address(id),
          asset_name varchar(64),
          issuer varchar(64),
          asset_type varchar(8),
          description varchar(64)
        )DEFAULT charset=utf8;
    '''

    tb_input = '''
        create table if not EXISTS tx_input(
          id bigint PRIMARY key,
          tx_id bigint REFERENCES tx(id),
          belong_tx_id bigint REFERENCES tx(id),
          tx_index bigint REFERENCES tx_output(output_index),
          tx_value bigint not null,
          script varchar(1024),
          address_id bigint REFERENCES address(id),
          asset varchar(64),
          decimal_number varchar(8)
        )DEFAULT charset=ascii;
    '''

    idx_block_hash = '''
        create index idx_block_hash on block(hash);
    '''

    idx_tx_hash = '''
        create index idx_tx_hash on tx(hash);
    '''
    idx_tx_input_tx_id = '''
        create index idx_tx_input_tx_id on tx_input(tx_id);
'''
    idx_tx_output_hash = '''
        create index idx_tx_output_hash on tx_output(hash);
'''

    idx_tx_input_address_id = '''
    create index idx_tx_input_address_id on tx_input(address_id);
'''

    idx_tx_output_address_id = '''
    create index idx_tx_output_address_id on tx_output(address_id);
'''
    sum_in = '''
        create view sum_input(value)
        as
        select sum(tx_value) from tx_input where asset = 'ETP';
    '''
    sum_out = '''
        create view sum_output(value)
        as
        select sum(output_value) from tx_output where asset = 'ETP';
    '''
    stat_day = '''
        create view stat_day(date, block_supply, transaction_count)
        as
        select date_format(from_unixtime(time_stamp),'%Y%m%d'),count(*) ,sum(transaction_count)-count(*) from block group by date_format(from_unixtime(time_stamp),'%Y%m%d');
    '''


    tables = []
    tables.append(tb_block)
    tables.append(tb_address)
    tables.append(tb_tx)
    tables.append(tb_output)
    tables.append(tb_output_asset)
    tables.append(tb_input)

    tables.append(idx_block_hash)
    tables.append(idx_tx_hash)
    tables.append(idx_tx_input_tx_id)
    tables.append(idx_tx_output_hash)
    tables.append(idx_tx_input_address_id)
    tables.append(idx_tx_output_address_id)
    tables.append(sum_in)
    tables.append(sum_out)
    tables.append(stat_day)
    tables.append(tb_block_fork)
    tables.append(tb_tx_fork)
    tables.append(tb_output_fork)
    tables.append(tb_input_fork)

    cur = conn.cursor()
    for tb in tables:
        try:
            cur.execute(tb)
        except Exception as e:
            logging.info('create sql exception,%s,%s' % (e, tb))
    cur.close()
    conn.commit()


def new_mongo(host, port, db_name_):
    conn = pymongo.MongoClient(host, port)
    try:
        conn.admin.command('ismaster')
    except ConnectionFailure as e:
        logging.error('mongodb connection error,%s' % e)
        raise e

    return getattr(conn, db_name_)


class RpcClient:

    def __init__(self, uri):
        self.__uri = uri

    def __request(self, cmd_, params):
        if not isinstance(params, list):
            raise RuntimeError('bad params type')

        cmd_ = cmd_.replace('_', '-')
        content = {'method':cmd_, 'params':params}
        resp = requests.post(self.__uri, data=json.dumps(content))
        if resp.status_code != 200:
            raise RuntimeError('bad request,%s' % resp.status_code)

        return resp.text

    def __getattr__(self, item):
        return lambda params:self.__request(item, params)


def new_rpcclient(uri):
    return RpcClient(uri)


def set_up_signal(code):
    def handle_sig(s, f):
        logging.info('receive signal %s, begin to exit...' % s)
        code(True)

    signal.signal(signal.SIGTERM, handle_sig)
    signal.signal(signal.SIGINT, handle_sig)


def dirty_worker(db_chain, block, transactions, addresses, outs, asset_outs, inputs):
    if not transactions or not outs or not inputs:
        raise RuntimeError('transaction,outputs, ')

    def insert_to_table(sql, data):
        db_chain.set_character_set('utf8')
        cur = db_chain.cursor()
        cur.execute('SET NAMES utf8;')
        cur.execute('SET CHARACTER SET utf8;')
        cur.execute('SET character_set_connection=utf8;')
        for d in data:
            cur.execute(sql, d)
        cur.close()

    try:
        sql = 'insert into block(hash, bits, transaction_count, mixhash, version, ' \
              'merkle_tree_hash, previous_block_hash, nonce, time_stamp, number) ' \
              'values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
        data = [(block['hash'], block['bits'], block['transaction_count'], block['mixhash'],
                block['version'], block['merkle_tree_hash'], block['previous_block_hash'], block['nonce'], block['time_stamp'],
                block['number'])]
        insert_to_table(sql, data)
        sql = 'insert into tx(id, block_height, hash) values(%s, %s, %s)'
        data = [(t['tx_id'], t['block_height'], t['hash']) for t in transactions]
        insert_to_table(sql, data)

        addrs = [{'address':a, 'id':a_id} for a, a_id in addresses.items()]

        if addrs:
            sql = 'insert into address(id, address) values(%s, %s)'
            data = [(addr['id'], addr['address']) for addr in addrs]
            insert_to_table(sql, data)

        sql = 'insert into tx_output(id, hash, tx_id, output_index, output_value' \
              ', address_id, script, asset, decimal_number) values(%s, %s, %s, %s, %s, %s, %s, %s, %s)'
        data = [(out['output_id'], out['hash'], out['tx_id'], out['index'],
                 out['value'], out['address_id'], out['script'], out['asset'], out['decimal_number']) for out in outs]
        insert_to_table(sql, data)
        sql = 'insert into tx_output_asset(id, hash, tx_id, output_index, output_value' \
              ', address_id, asset_name, issuer, asset_type, description) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

        '''id bigint PRIMARY key,
          hash char(64) NOT NULL ,
          tx_id bigint REFERENCES tx(id),
          output_index bigint not null,
          output_value bigint,
          address_id bigint REFERENCES address(id),
          issuer varchar(64),
          asset_type varchar(8),
          description varchar(64),'''

        data = [(out['output_id'], out['hash'], out['tx_id'], out['index'],
                 out['value'], out['address_id'], out['asset'], out['issuer'], out['asset_type'], out['description']) for out in asset_outs]
        logging.info(str(data))
        insert_to_table(sql, data)

        sql = 'insert into tx_input(id, tx_id, belong_tx_id, tx_index, tx_value, script, address_id, asset, decimal_number) values(%s, %s, %s, %s, %s, %s, %s, %s, %s)'
        data = [(i['input_id'], i['tx_id'], i['belong_tx_id'], i['output_index'], i['tx_value'], i['script'], i['address_id'], i['asset'], i['decimal_number']) for i in inputs]
        insert_to_table(sql, data)

        db_chain.commit()

    except Exception as e:
        logging.error('mongodb exception,%s' % e)


def dirty_worker_(db_chain, block, transactions, addresses, outs, inputs):
    if not transactions or not outs or not inputs:
        raise RuntimeError('transaction,outputs, ')

    try:
        tb_block = db_chain.block
        tb_block.insert_one(block)

        tb_tx = db_chain.transaction
        tb_tx.insert_many(transactions)

        addrs = [{'address':a, 'id':a_id} for a, a_id in addresses.items()]

        if addrs:
            tb_addr = db_chain.address
            tb_addr.insert_many(addrs)

        tb_output = db_chain.output
        tb_output.insert_many(outs)

        tb_input = db_chain.input
        tb_input.insert_many(inputs)

    except Exception as e:
        logging.error('mongodb exception,%s' % e)


def input_hash_to_output_id_(db_chain, hashes):
    hashes = filter(lambda x: False if x == null_hash else True, hashes)
    if not hashes:
        return {null_hash: [-1, 4294967295]}
    cur = db_chain.transaction.find({'hash': {'$in': hashes}})

    for c in cur:
        print c
    res = {c['hash']:c['tx_id'] for c in cur}
    res[null_hash] = -1
    return res

def get_gloab_asset(db_chain):
    curl = db_chain.cursor()
    sql = 'select asset_name , asset_type from tx_output_asset'
    curl.execute( sql )
    rds = curl.fetchall()
    for rd in rds:
        gloab_asset[rd[0]] = rd[1]

def input_hash_to_output_id(db_chain, hashes):
    hashes = filter(lambda x: False if x[0] == null_hash else True, hashes)
    if not hashes:
        return {}
    hashes_ = ['\'%s\'' % h for h, _ , _ in hashes]
    cur = db_chain.cursor()
    sql = 'select hash, tx_id, address_id, output_value, output_index, asset, decimal_number from tx_output where hash in (%s)' % (','.join(hashes_))
    cur.execute( sql )
    rds = cur.fetchall()
    res = {}
    for rd in rds:
        found = False
        for h, idx, name in hashes:
            if rd[4] == idx and rd[5] == name:
                found = True
                break
        if not found:
            continue
        res['%s_%s_%s' % (rd[0], rd[4], rd[5])] = [rd[1], rd[2], rd[3], rd[4], rd[5], rd[6]]
    cur.close()
    return res


def rpc_network_check(func):
    def wrapper(*args, **kwargs):
        try:
            res = func(*args, **kwargs)
        except Exception as e:
            global max_try
            logging.info('try %s' % max_try)
            max_try -= 1
            if max_try <= 0:
                raise RpcConnectionException('after 10 tries')
            raise TryException('try %s' % max_try)
        if res.find('timed out') > 0:
            raise RpcConnectionException('%s request failed,block height %s' % (func.__name__, block_height))
        return res
    return wrapper


@rpc_network_check
def get_header(rpc, height):
    return rpc.fetch_header(['-t', str(height)])


@rpc_network_check
def get_block(rpc, hash_, json_or_not = False):
    return rpc.getblock([hash_, '--json=%s' % json_or_not])


def sync_block(db_chain, block_height, tx_id, o_id, i_id, addresses, rpc):
    addresses_ = {}
    #for key in gloab_asset:
    #   print(key, gloab_asset[key])
    res = get_header(rpc, block_height)
    try:
        res = json.loads(res)
    except Exception as e:
        logging.info('getheader json loads exception,%s,%s, %s' % (block_height, e, res))
        raise HeaderNotFound('header %s not found' % block_height)
    hash_ = res['result']['hash']

    block_ = get_block(rpc, hash_, 'true')
    block_ = json.loads(block_)

    header = block_['header']['result']
    header['number'] = int(header['number'])
    txs_ = block_['txs']['transactions']
    txs = [tx['hash'] for tx in txs_]

    header['txs'] = txs
    block = header

    transactions = []
    outs = []
    asset_outs = []
    inputs = []
    asset_pre = ['ETP']
    hashes = []
    block_tx_hash = {}

    block_tx_output_value = {}
    for tx in txs_:
        transactions_ = {}
        tx_hash = tx['hash']
        transactions_['hash'] = tx_hash

        block_tx_hash[tx_hash] = tx_id

        transactions_['tx_id'] = tx_id


        outputs = tx['outputs']
        inputs_ = tx['inputs']

        for o in outputs:
            if 'asset-transfer' == o['attachment']['type']:
                asset_name = o['attachment']['symbol']
                asset_pre.append(asset_name)

        #print("asset_pre:")
        #print(asset_pre)
        new_asset_pre = list(set(asset_pre))
        #print("new:")
        #print(new_asset_pre)
        for i in inputs_:
            for a in new_asset_pre:
                hashes.append((i['previous_output']['hash'], int(i['previous_output']['index']), a))
        #print("hashes:")
        #print(hashes)
        #hashes = [(i['previous_output']['hash'], int(i['previous_output']['index'])) for i in inputs_]
        hash_ids = input_hash_to_output_id(db_chain, hashes)
        for i in inputs_:
            for a in new_asset_pre:
                p = i['previous_output']
                pre_hash = p['hash']
                pre_index = int(p['index'])
                hash_index = '%s_%s_%s' % (pre_hash, pre_index, a)
                if pre_hash != null_hash and hash_index not in hash_ids and hash_index not in block_tx_output_value:
                    #raise CriticalException('previous output hash not found,%s', p)
                    continue

                if pre_hash == null_hash:
                    pre_ids = [-1, -1, 0, -1, 'ETP', '8']
                else:
                    if hash_index in hash_ids:
                        pre_ids = hash_ids[hash_index]
                    else:
                        pre_ids = block_tx_output_value[hash_index]
                    # pre_ids = [tx_id_value[0], tx_id_value[1], pre_hash, tx_id_value[2]]

            # pre_ids = hash_ids[pre_hash] if pre_hash in hash_ids else [-1, -1, 0]
            # pre_tx_id = block_tx_hash[pre_hash] if pre_hash in block_tx_hash else pre_ids[0]
                pre_tx_id = pre_ids[0]
                address_id = pre_ids[1]
                tx_value = pre_ids[2]
                asset = pre_ids[4]
                decimal_number = pre_ids[5]
                inputs.append({'input_id': i_id, 'script': i['script'], 'belong_tx_id': pre_tx_id, 'tx_value': tx_value, 'address_id':address_id, 'output_index': pre_index, 'tx_id':tx_id, 'asset':asset, 'decimal_number':decimal_number})
                i_id += 1

        for o in outputs:
            if o.get('address') is None:
                addr = ''
            else:
                addr = o['address']
            if addr not in addresses:
                a_id = len(addresses) + 1
                addresses[addr] = a_id
                addresses_[addr] = a_id
            else:
                a_id = addresses[addr]
            #block_tx_output_value['%s_%s' % (tx_hash, o['index'])] = (tx_id, a_id, int(o['value']), int(o['index']))

            if o['attachment']['type'] == 'asset-issue':
                asset_name = o['attachment']['symbol']
                asset_amount = int(o['attachment']['quantity'])
                gloab_asset[asset_name] = o['attachment']['decimal_number']
                asset_outs.append({'hash': tx_hash, 'address_id': a_id
                                      , 'output_id': o_id, 'value': asset_amount
                                      , 'asset': asset_name, 'index': int(o['index']), 'tx_id': tx_id
                                      , 'description': o['attachment']['description']
                                      , 'asset_type': o['attachment']['decimal_number'],
                                   'issuer': o['attachment']['issuer']})
                outs.append({'hash': tx_hash, 'address_id': a_id, 'decimal_number': o['attachment']['decimal_number'],
                             'script': o['script'], 'output_id': o_id, 'value': asset_amount, 'asset': asset_name,
                             'index': int(o['index']), 'tx_id': tx_id})
                block_tx_output_value['%s_%s_%s' % (tx_hash, o['index'], asset_name)] = (
                tx_id, a_id, int(o['attachment']['quantity']), int(o['index']), asset_name,
                o['attachment']['decimal_number'])
                if o['value'] == '0':
                    o_id += 1
                    continue
                o_id += 1
            elif 'asset-transfer' == o['attachment']['type']:
                asset_name = o['attachment']['symbol']
                asset_amount = int(o['attachment']['quantity'])
                asset_pre.append(asset_name)
                outs.append({'hash': tx_hash, 'address_id': a_id, 'script': o['script'], 'output_id': o_id,
                             'value': asset_amount
                                , 'asset': asset_name, 'index': int(o['index']), 'tx_id': tx_id,
                             'decimal_number': gloab_asset[asset_name]})
                block_tx_output_value['%s_%s_%s' % (tx_hash, o['index'], asset_name)] = (
                    tx_id, a_id, int(asset_amount), int(o['index']), asset_name, gloab_asset[asset_name])
                o_id += 1
                if o['value'] == '0':
                    continue
            asset_name = 'ETP'
            outs.append(
                {'hash': tx_hash, 'address_id': a_id, 'script': o['script'], 'output_id': o_id, 'value': o['value'],
                 'asset': asset_name, 'index': int(o['index']), 'tx_id': tx_id, 'decimal_number': '8'})
            block_tx_output_value['%s_%s_%s' % (tx_hash, o['index'], asset_name)] = (
                tx_id, a_id, int(o['value']), int(o['index']), asset_name, '8')
            o_id += 1



        transactions_['block_height'] = block_height
        transactions.append(transactions_)
        tx_id += 1

    return block, transactions, addresses_, outs, asset_outs, inputs


def process_batch(db_chain, batch_info):
    block_height = batch_info['block_height']
    tx_id = batch_info['tx_id']
    input_id = batch_info['input_id']
    output_id = batch_info['output_id']
    try:
        db_chain.block.remove({'number': {'$gte': block_height}})
        db_chain.transaction.remove({'tx_id': {'$gte': tx_id}})
        db_chain.output.remove({'output_id': {'$gte': output_id}})
        db_chain.input.remove({'input_id': {'$gte': input_id}})
    except Exception as e:
        logging.info('process batch exception,%s' % e)
        raise e


def get_last_height(db_chain):

    cur = db_chain.cursor()
    def get_last_line_of_field(tb, field):
        res = cur.execute('select %s from %s order by %s desc limit 1' % (field, tb, field) )
        if res < 1:
            return -1
        return cur.fetchall()[0][0]

    block_height = get_last_line_of_field('block', 'number')
    max_tx_id = get_last_line_of_field('tx', 'id')
    max_output_id = get_last_line_of_field('tx_output', 'id')
    max_input_id = get_last_line_of_field('tx_input', 'id')
    cur.close()

    return block_height+1, max_tx_id+1, max_output_id+1, max_input_id+1


def get_last_height_(db_chain):
    batches = db_chain.batch.find()
    map(lambda x: process_batch(db_chain, x), batches)

    db_chain.batch.remove()

    block_height = db_chain.block.find({}).sort('number', -1).limit(1)
    max_tx_id = db_chain.transaction.find().sort('tx_id', -1).limit(1)
    max_output_id = db_chain.output.find({}).sort('output_id', -1).limit(1)
    max_input_id = db_chain.input.find({}).sort('input_id', -1).limit(1)

    block_height_count = block_height.count()

    return block_height.next()['number'] + 1 if block_height_count > 0  else 0\
        , max_tx_id.next()['tx_id'] + 1 if max_tx_id.count() > 0 else 0\
        , max_output_id.next()['output_id'] + 1 if max_output_id.count() > 0 else 0\
        , max_input_id.next()['input_id'] + 1if max_input_id.count() > 0 else 0


def send_sms(text, phone):
    """
    能用接口发短信
    """
    import time
    print('%s,%s,%s' % (text, phone, time.ctime()) )
    text = "【海枫藤数字资产】%s" % text
    params = urllib.urlencode({'un': un, 'pw' : pw, 'msg': text, 'phone':phone, 'rd' : '1'})
    headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
    conn = httplib.HTTPConnection(host, port=port, timeout=30)
    conn.request("POST", sms_send_uri, params, headers)
    response = conn.getresponse()
    response_str = response.read()
    conn.close()
    return response_str

def fork_db(db_chain, blocks, txs, outputs, inputs):

    def insert_to_table(sql, data):
        cur = db_chain.cursor()
        for d in data:
            cur.execute(sql, d)
        cur.close()

    try:
        sql = 'insert into block_fork(hash, bits, transaction_count, mixhash, version, ' \
              'merkle_tree_hash, previous_block_hash, nonce, time_stamp, number) ' \
              'values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

        data = [(block['hash'], block['bits'], block['transaction_count'], block['mixhash'], block['version'], block['merkle_tree_hash'], block['previous_block_hash'], block['nonce'], block['time_stamp'], block['number']) for block in blocks]
        insert_to_table(sql, data)

        sql = 'insert into tx_fork(id, block_height, hash) values(%s, %s, %s)'
        data = [(t['id'], t['block_height'], t['hash']) for t in txs]
        insert_to_table(sql, data)

        sql = 'insert into tx_output_fork(id, hash, tx_id, output_index, output_value' \
              ', address_id, script, asset) values(%s, %s, %s, %s, %s, %s, %s, %s)'
        data = [(out['id'], out['hash'], out['tx_id'], out['output_index'],
                 out['out_value'], out['address_id'], out['script'], out['asset']) for out in outputs]
        insert_to_table(sql, data)


        sql = 'insert into tx_input_fork(id, tx_id, belong_tx_id, tx_index, tx_value, script, address_id) values(%s, %s, %s, %s, %s, %s, %s)'
        data = [(i['id'], i['tx_id'], i['belong_tx_id'], i['tx_index'], i['tx_value'], i['script'], i['address_id']) for i in inputs]
        insert_to_table(sql, data)

        db_chain.commit()

    except Exception as e:
        logging.error('mongodb exception,%s' % e)

def record_block_fork(db_chain, fork_begin, fork_end):
    outs = []
    inputs = []
    blocks = []
    txs = []
    height = -1
    tx_count = 0
    total_blocks = 0
    try:
        curl = db_chain.cursor()
        if fork_end > 0 and fork_end >fork_begin:
            sql = 'select * from tx, block where block.number = tx.block_height and tx.block_height >= %s and tx.block_height <= %s' % (fork_begin, fork_end)
        elif fork_end == -1:
            sql = 'select * from tx, block where block.number = tx.block_height and tx.block_height >= %s' % fork_begin
        curl.execute(sql)
        rds = curl.fetchall()

        for rd in rds:
            if rd[1] != height:
                tx_count = 0
                total_blocks += 1
                height = rd[1]
                logging.info('\n')
                logging.info("@Height  : %s ", rd[1])
                logging.info("Block Hash : %s", rd[4])
                logging.info("Block difficulty : %s", rd[5])
                logging.info("Block transaction count : %s", rd[6])
                logging.info("Block mixhash : %s", rd[7])
                logging.info("version :%s", rd[8])
                logging.info("merkle hash :%s", rd[9])
                logging.info("previous_hash :%s", rd[10])
                logging.info("Block nonce :%s", rd[11])
                logging.info("Block timestamp :%s", rd[12])
                blocks.append({'hash':rd[4], 'bits':rd[5], 'transaction_count':rd[6], 'mixhash':rd[7], 'version':rd[8], 'merkle_tree_hash':rd[9], 'previous_block_hash': rd[10], 'nonce': rd[11], 'time_stamp':rd[12], 'number':rd[1]})
            tx_count += 1
            txs.append({'id': rd[0], 'block_height': rd[1], 'hash': rd[2]})
            sql = 'select * from tx_input where tx_id = %s' % rd[0]
            curl.execute(sql)
            rds_in = curl.fetchall()
            logging.info('Number %s tx:' % str(tx_count))
            logging.info('tx hash: %s' % str(rd[2]))
            for rd_in in rds_in:
                inputs.append({'id':rd_in[0], 'tx_id':rd_in[1], 'belong_tx_id':rd_in[2], 'tx_index':rd_in[3], 'tx_value':rd_in[4], 'script':rd_in[5], 'address_id':rd_in[6]})
                if rd_in[6] != -1:
                    sql1 = "select address from address where id = %s" % rd_in[6]
                    curl.execute(sql1)
                    rds_adr = curl.fetchall()
                    logging.info("input address : %s ,input value %s " % (rds_adr, rd_in[4]))

            sql = 'select * from tx_output, address where tx_output.address_id = address.id and tx_id =%s' % rd[0]
            curl.execute(sql)
            rds_out = curl.fetchall()
            for rd_out in rds_out:
                logging.info('output address: %s ,output value: %s' % (rd_out[9],rd_out[4]))
                outs.append({'id': rd_out[0], 'hash': rd_out[1], 'tx_id': rd_out[2], 'output_index': rd_out[3], 'out_value': rd_out[4], 'address_id': rd_out[5], 'script': rd_out[6], 'asset': rd_out[7]})
        phone_sun = '17715917889'
        phone_jiang = '18321969032'
        text = 'Our etp chain block is forking from height %s, total block %s! Dangerous!' % (fork_begin, str(total_blocks))
        if total_blocks > 5:
            send_sms(text, phone_sun)
    except Exception as e:
        logging.info('record fork exception,%s' % e)
    return blocks, txs, outs, inputs


def clear_fork(db_chain, fork_height):
    tx_id = -1
    tx_input_id = -1
    tx_output_id = -1
    try:
        blocks_f ,txs_f , outs_f, inputs_f = record_block_fork(db_chain, fork_height, -1)
        fork_db(db_chain, blocks_f, txs_f, outs_f, inputs_f)
        cur = db_chain.cursor()
        sql = 'select min(id) from tx where block_height=%s' % fork_height
        cur.execute(sql)
        rds = cur.fetchall()
        if rds:
            tx_id = rds[0][0]

        sql = 'select min(id) from tx_input where tx_id =%s' % tx_id
        cur.execute(sql)
        rds = cur.fetchall()
        if rds:
            tx_input_id = rds[0][0]

        sql = 'select min(id) from tx_output where tx_id=%s' % tx_id
        cur.execute(sql)
        rds = cur.fetchall()
        if rds:
            tx_output_id = rds[0][0]

        tbs = collections.OrderedDict()
        tbs['tx_input'] = tx_input_id
        tbs['tx_output'] = tx_output_id
        tbs['tx'] = tx_id
        # tbs['block'] = fork_height
        for tb, id_ in tbs.items():
            sql = 'delete from %s where id >= %s' % (tb, id_)
            res = cur.execute(sql)
            logging.info('clear %s from %s, effected,%s' % (tb, id_, res))
        sql = 'delete from block where number >= %s' % fork_height
        res = cur.execute(sql)
        logging.info('clear block from %s, effected,%s' % (fork_height, res))
        cur.close()
        db_chain.commit()
    except Exception as e:
        logging.info('clear fork exception,%s' % e)


def process_fork(db_chain, rpc, current_height):
    height = current_height
    # if height % 20 > 0:
    #     return

    def check_block(hash):
        cur = db_chain.cursor()
        sql = 'select * from block where hash = \'%s\'' % hash
        cur.execute(sql)
        rds = cur.fetchall()
        cur.close()
        if not rds:
            return False
        return True

    fork_height = -1
    while True:
        try:
            header = get_header(rpc, height)
            header = json.loads(header)
            hash = header['result']['hash']

            # if height % 10 == 9:
            #     hash = 'fuck'
            existed = check_block(hash)
            if not existed:
                fork_height = height
            else:
                if current_height == height:
                    height -= 1
                    continue
                break
            if height == 1:
                break
            height -= 1
        except Exception as e:
            logging.info('process fork exception,%s,%s' % (e, height))
            break
    if fork_height >= 0:
        logging.info('fork at %s' % fork_height)
        clear_fork(db_chain, fork_height)


def workaholic(stopped):
    # db_chain = new_mongo(mongodb_host, mongodb_port, db_name)
    # names = db_chain.collection_names()
    # logging.info(names)

    db_chain = new_mysql(host, port, user, passwd, db_name)
    init_table(db_chain)
    rpc = new_rpcclient(rpc_uri)

    last_height = 0
    get_gloab_asset(db_chain)

    def latest_addresses_(db_chain_):
        addresses = db_chain_.address.find().sort('id', 1)
        addresses_ = collections.OrderedDict()
        for addr in addresses:
            addresses_[addr['address']] = addr['id']
        return  addresses_

    def latest_addresses(db_chain_):
        addresses_ = collections.OrderedDict()
        cur = db_chain_.cursor()
        cur.execute('select id, address from address')
        rds = cur.fetchall()
        for rd in rds:
            addresses_[rd[1]] = rd[0]
        cur.close()
        return addresses_

    def batch_begin(db_chain_, batch_info):
        return
        db_chain_.batch.insert_one(batch_info)

    def batch_end(db_chain_, batch_info):
        return
        db_chain_.batch.remove(batch_info)

    addresses = latest_addresses(db_chain)

    while not stopped():
        block_height = -1
        try:
            block_height, tx_id, o_id, i_id = get_last_height(db_chain)
            if last_height == block_height:
                time.sleep(1)
            block, transactions, addresses_, outs, asset_outs, inputs = sync_block(db_chain, block_height, tx_id, o_id, i_id, addresses, rpc)
            output_line = 'block,%s, tx,%s,address,%s, output,%s, input, %s' % \
                          (block['number'], len(transactions), len(addresses_), len(outs), len(inputs))

            logging.info(output_line)

            batch_info = {'block_height': block_height, 'tx_id':tx_id, 'output_id':o_id, 'input_id':i_id}
            batch_begin(db_chain, batch_info)
            dirty_worker(db_chain, block, transactions, addresses_, outs, asset_outs, inputs)
            batch_end(db_chain, batch_info)
        except TryException as e:
            pass
        except HeaderNotFound as e:
            logging.info('header not found except,%s' % e)
            time.sleep(24)
            continue
        except RpcConnectionException as e:
            logging.error('rpc network problem,%s' % e)
            break
        except int as e:
            logging.error('workholic exception,%s' % e)
        process_fork(db_chain, rpc, block_height)
        if block_height > 0:
            last_height = block_height

    logging.info('service begin to exit...')


def do_clear(args):
    db_chain = new_mysql(host, port, user, passwd, db_name)
    cur = db_chain.cursor()
    sqls = []

    sqls.append('drop view stat_day;')
    sqls.append('drop view sum_input;')
    sqls.append('drop view sum_output;')
    sqls.append('drop table tx_input;')
    sqls.append('drop table tx_output;')
    sqls.append('drop table tx_output_asset;')
    sqls.append('drop table address;')
    sqls.append('drop table tx;')
    sqls.append('drop table block;')
    sqls.append('drop table block_fork;')
    sqls.append('drop table tx_fork;')
    sqls.append('drop table tx_output_fork;')
    sqls.append('drop table tx_input_fork;')
    for sql in sqls:
        try:
            cur.execute(sql)
        except Exception as e:
            logging.info(e)

    cur.close()
    db_chain.commit()
    db_chain.close()


def do_check(args):
    pass


def do_fork(args):
    assert(len(args) == 1)
    db_chain = new_mysql(host, port, user, passwd, db_name)
    clear_fork(db_chain, int(args[0]))


def do_help(args):
    print(
'''Usage:python block_sync.py action [options...]
action:
    clear    clear all database
    help''')

import sys

def main(argv):
    reload(sys)
    sys.setdefaultencoding('utf-8')
    print sys.getdefaultencoding()
    if len(argv) > 1:
        action = sys.argv[1]
        cmd_action = {'clear': do_clear, 'help': do_help, 'check': do_check, 'fork': do_fork}
        if action in cmd_action:
            cmd_action[action](argv[2:])
            return
        else:
            do_help(argv[2:])
            return

    logging.info('service begin...')
    stopped = Boolean()
    set_up_signal(stopped)
    workaholic(stopped)


if __name__ == '__main__':
    main(sys.argv)
