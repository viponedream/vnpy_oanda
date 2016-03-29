# encoding: UTF-8

'''一个简单的OandaAPI客户端，主要使用requests开发，主要作用是下载oanda行情历史数据'''

import requests
import json
import os
import time
import datetime
import pymongo

API_SETTING = {}
API_SETTING['practice'] = {'rest': 'https://api-fxpractice.oanda.com',
                           'stream': 'https://stream-fxpractice.oanda.com'}
API_SETTING['trade'] = {'rest': 'https://api-fxtrade.oanda.com',
                        'stream': 'https://stream-fxtrade.oanda.com/'}
API_SETTING['version'] = 'v1'
FILENAME = os.path.join(os.path.abspath(".."), "OandaGateway", "OANDA_connect.json")

HTTP_OK = 200


########################################################################
class OandaClient(object):
    """Oanda客户端"""

    name = u'Oanda数据下载客户端'

    # ----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.token = ''  # 授权码
        self.accountId = ''  # 账户id
        self.settingName = ''  # "practice","trade"
        self.domain = ''  # 主域名
        self.version = ''  # API版本
        self.settingLoaded = False  # 配置是否已经读取
        self.dbClient = pymongo.MongoClient()#初始化MongoClient
        self.loadSetting()  # 加载配置

        # self.active = False    # API的工作状态

    def loadSetting(self):
        """载入配置"""
        try:
            f = file(FILENAME)
        except IOError:
            print u'%s无法打开配置文件' % self.name
            return

        setting = json.load(f)
        try:
            self.token = str(setting['token'])
            self.accountId = str(setting['accountId'])
            self.settingName = str(setting['settingName'])
            self.domain = API_SETTING[self.settingName]['rest']
            self.version = API_SETTING['version']
        except KeyError:
            print u'%s配置文件字段缺失' % self.name
            return

        self.settingLoaded = True
        print u'%s配置载入完成' % self.name

    def downloadSymbolList(self):
        '''这个方法还不行，因为accountId似乎不能用，正联系客服开通中 20160321'''
        path = 'instruments'
        params = {}
        params['accountId'] = self.accountId
        return self._downloadData(path, params)

    def _downloadHData(self, symbol, start, end):
        '''给定一个品种，一个起始datetime，一个终止datetime'''
        path = 'candles'
        params = {}
        # instrument=EUR_USD&count=2&candleFormat=midpoint&granularity=D
        # instrument=EUR_USD&start=2014-06-19T15%3A47%3A40Z&end=2014-06-19T15%3A47%3A50Z"
        params['instrument'] = symbol  # 品种
        params['start'] = start  # 字符串 2002-06-16T10:47:40Z
        params['end'] = end  # 字符串 2002-06-16T10:47:40Z
        params['granularity'] = 'M1'
        return self._downloadData(path, params)

    def _downloadData(self, path, params):
        """下载数据"""
        if not self.settingLoaded:
            print u'%s配置未载入' % self.name
            return None
        else:
            url = '/'.join([self.domain, self.version, path])
            r = requests.get(url=url, params=params)

            if r.status_code != HTTP_OK:
                print u'%shttp请求失败，状态代码%s' % (self.name, r.status_code)
                return None
            else:
                result = r.json()
                return result['candles']

if __name__ == '__main__':
    client = OandaClient()
    '''
    print client.token  # 授权码
    print client.accountId  # 账户id
    print client.settingName  # "practice","trade"
    print client.domain  # 主域名
    print client.version  # API版本
    print client.settingLoaded  # 配置是否已经读取
    # print client.downloadSymbolList()#下载所有品种列表
    '''
    #data0= client._downloadHData("EUR_USD", '2012-06-16T10:47:40Z', '2012-06-18T10:47:40Z')
    #print data0[0]
    print client.downloadHistoricData("EUR_USD", '2012-06-16 10:47:40', '2012-06-18 10:47:40')
    ''', '2014-06-19T15%3A47%3A50Z', '2014-06-19T16:47:50Z'
    https://api-fxpractice.oanda.com/v1/candles?instrument=EUR_USD
    &granularity=M1&start=2002-06-16T10%3A47%3A40Z&end=2002-06-19T15%3A47%3A50Z
    '''
