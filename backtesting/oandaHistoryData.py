# encoding: UTF-8

"""
本模块中主要包含：
1. 从Oanda下载历史行情的引擎
"""
import datetime
from datetime import timedelta
import time
import pymongo

from oandaClient import OandaClient

from ctaBase import *
from vtConstant import *

########################################################################
class HistoryDataEngine(object):
    """CTA模块用的历史数据引擎"""
    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.dbClient = pymongo.MongoClient()
        self.oandaClient = OandaClient()
    def downloadSignalMinuteBar(self,symbol,start,end):
        '''oandaClient的方法　给定品种，起止时间（格式：'2012-06-16 10:47:40'），下载数据并写入以__命名的db中的以___命名的collection'''
        def parseTimeStr(timestr):
            t = time.strptime(timestr, "%Y-%m-%d %H:%M:%S")
            dt = datetime.datetime(*t[:6])
            return dt
        def insertData(dbname,symbol,data):
            if data:
                # 创建datetime索引
                self.dbClient[MINUTE_DB_NAME][symbol].ensure_index([('datetime', pymongo.ASCENDING)],unique=True)
                for d in data:
                    bar=OandaBarData()
                    bar.symbol=symbol
                    try:
                        bar.openBid = d.get("openBid")             # OHLC
                        bar.openAsk = d.get("openAsk")
                        bar.highBid = d.get("highBid")
                        bar.highAsk = d.get("highAsk")
                        bar.lowBid = d.get("lowBid")
                        bar.lowAsk = d.get("lowAsk")
                        bar.closeBid = d.get("closeBid")
                        bar.closeAsk = d.get("closeAsk")

                        timestr=d.get("time")
                        t = time.strptime(timestr[:18], "%Y-%m-%dT%H:%M:%S")
                        dt = datetime.datetime(*t[:6])
                        bar.time = dt    # 时间 先设置成一样的，我不知道这个是用来做什么的
                        bar.datetime = dt # python的datetime时间对象

                    except KeyError:
                        print d
                    flt = {'datetime': bar.datetime}
                    self.dbClient[MINUTE_DB_NAME][symbol].update_one(flt, {'$set':bar.__dict__}, upsert=True)
            else:
                print u'找不到合约%s' %symbol



        end = parseTimeStr(end)
        start = parseTimeStr(start)
        HISTORYDATABLOCKSIZE = datetime.timedelta(days=3,minutes=670)
        while end - start > HISTORYDATABLOCKSIZE:
            start_str=start.strftime("%Y-%m-%dT%H:%M:%SZ")
            end_str=(start + HISTORYDATABLOCKSIZE).strftime("%Y-%m-%dT%H:%M:%SZ")
            data=self.oandaClient._downloadHData(symbol, start_str, end_str)#逐个block进行下载
            insertData(MINUTE_DB_NAME,symbol,data)
            start = start + HISTORYDATABLOCKSIZE
        start_str=start.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_str=end.strftime("%Y-%m-%dT%H:%M:%SZ")
        data=self.oandaClient._downloadHData(symbol,start_str, end_str)#对最后的部分进行下载
        insertData(MINUTE_DB_NAME,symbol,data)
        print u'下载完成%s' %symbol

    def interpolateMinuteBar(self,symbol,method):
        """用插值法对MinuteBar进行插值计算,可以选择插值方法"""
        """问题点： 如何判定某个空档属于数据缺失，而不是休市时间"""
        """方法：看一下时间的差值，如果大于某个阈值，则判定为休市时间，暂定为1小时"""
        """想法是先读到pandas里面，然后进行插值"""
        """pandas.Series.interpolate"""
        """初步想法，先过一遍整个MinuteBar df, 对forward方向每个元素求一遍对前面的差值，如果大于阈值，则做标记，然后再回来去掉有标记的"""
        """我改主意了，直接在MongoDB里面改就是了，逐条读"""
        pass

    def resampleMinuteBar(self,symbol,toPeriod):
        """对M1数值进行降采样"""
        """pandas.DataFrame.resample"""
        pass

#----------------------------------------------------------------------

if __name__ == '__main__':
    ## 简单的测试脚本可以写在这里
    from time import sleep
    e = HistoryDataEngine()
    sleep(1)
    e.downloadSignalMinuteBar(symbol="EUR_USD",start="2012-01-01 00:00:00",end="2012-02-01 00:00:00")