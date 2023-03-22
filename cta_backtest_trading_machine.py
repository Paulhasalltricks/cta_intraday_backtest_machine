# -*- coding: utf-8 -*-
"""
Created on Mon Aug  1 13:55:29 2022

@author: South
"""

import pandas as pd
import os
import re
import time
from multiprocessing import Pool
import multiprocessing
import numpy as np
from common_tools import Common_Tools
import sys
sys.path.append('E:/machineLearning/ma_enhanced_project/code/trading_trigger/fiveTenMA_bolling/')



class Basic_CTA_Trading_Rule():
    
    def __init__(self):
        pass
    
    def calculate_ma(self,
                     maType,
                     lookBackLength,
                     freqMuti,
                     series):
        # 均线计算
        realFreq = lookBackLength*freqMuti
        if maType == 'MA':
            # 简单均线
            ma = series.rolling(realFreq).mean()
            ma = [i for i in ma]
            return ma
        elif maType == 'EMA':
            # EMA
            ema = pd.DataFrame.ewm(series,span=realFreq).mean()
            ema = [i for i in ema]
            return ema
    
    def sew_up(self,
               dfYesterday,
               dfToday,
               referedShortMaName,
               referedLongMaName):
        # 昨收时间段价格数据和今开“缝合”
        finalPositionYesterday = dfYesterday[referedShortMaName].iloc[-1] - dfYesterday[referedLongMaName].iloc[-1]
        openSituation = dfToday['LastPx'].dropna().iloc[0] - dfYesterday['LastPx'].dropna().iloc[-1]
        signal = finalPositionYesterday*openSituation
        if signal > 0:
            multiplier = dfToday['LastPx'].dropna().iloc[0]/dfYesterday['LastPx'].dropna().iloc[-1]
            return multiplier
        else:
            multiplier = 1
            return multiplier
    
    def calculate_ma_pairs_for_trading_day(self,
                                           dfYesterday,
                                           dfToday,
                                           shortMaName,
                                           longMaName,
                                           freqMuti):
        # 今日计算均线值及对应信号
        tempYesterday = dfYesterday.copy()
        shortMaLength = re.findall(r"\d+",shortMaName)[0]
        shortMaType = shortMaName[:-(len(shortMaLength)+1)]
        shortMaLength = int(shortMaLength)
        tempYesterday[shortMaName] = self.calculate_ma(shortMaType,
                                                       shortMaLength,
                                                       freqMuti,
                                                       tempYesterday['LastPx'])
        longMaLength = re.findall(r"\d+",longMaName)[0]
        longMaType = longMaName[:-(len(longMaLength)+1)]
        longMaLength = int(longMaLength)
        tempYesterday[longMaName] = self.calculate_ma(longMaType,
                                                      longMaLength,
                                                      freqMuti,
                                                      tempYesterday['LastPx'])
        multiplier = self.sew_up(tempYesterday,
                                 dfToday,
                                 shortMaName,
                                 longMaName)
        yesterdayReference = tempYesterday['LastPx']*multiplier
        tempToday = dfToday.copy()
        wholeSeries = pd.concat([yesterdayReference,tempToday['LastPx']])
        shortMaResult = self.calculate_ma(shortMaType,
                                          shortMaLength,
                                          freqMuti,
                                          wholeSeries)
        longMaResult = self.calculate_ma(longMaType,
                                         longMaLength,
                                         freqMuti,
                                         wholeSeries)
        tempToday[shortMaName] = shortMaResult[len(tempYesterday):]
        tempToday[longMaName] = longMaResult[len(tempYesterday):]
        maRelationColName = 'basicRelativePositionOf' + shortMaName + '_' + longMaName
        tempToday[maRelationColName] = [self.basic_relative_position_of_ma(tempToday[shortMaName].iloc[i],tempToday[longMaName].iloc[i]) for i in range(len(tempToday))]
        return tempToday
        
    def basic_relative_position_of_ma(self,
                                      shortPeriodMa,
                                      longPeriodMa):
        # 均线信号
        if shortPeriodMa - longPeriodMa > 0.00001:
            # 短均线在长均线之上，输出1
            return 1
        elif shortPeriodMa - longPeriodMa < 0.00001:
            # 短均线在长均线之下，输出-1
            return -1
        else:
            return 0
        
    def intialization_of_a_trading_process(self,
                                           intiDateTimeMark):
        # tradingLog 的第一条记录
        dateTimeMark = intiDateTimeMark
        px = 0
        position = 0
        cash = 0 
        tradeCost = 0
        cumTradeCost = 0
        delta = 0
        netValue = 0
        intiRow = pd.DataFrame([[dateTimeMark,
                                 px,
                                 position,
                                 cash,
                                 tradeCost,
                                 cumTradeCost,
                                 delta,
                                 netValue]],
                               columns=['dateTimeMark',
                                        'px',
                                        'position',
                                        'cash',
                                        'tradeCost',
                                        'cumTradeCost',
                                        'delta',
                                        'netValue'])
        return intiRow
    
    def oneTradeCostGenerater(self,
                              tradeCostMode,
                              singleOrBoth,
                              tradeCostReference,
                              px,
                              delta):
        # 计算单笔交易的tradeCost
        if singleOrBoth == 'single':
            realTradeCostReference = tradeCostReference/2
        elif singleOrBoth == 'both':
            realTradeCostReference = tradeCostReference
        if tradeCostMode == 'relative':
            oneTradeCost = px * abs(delta) * realTradeCostReference
            return oneTradeCost
        elif tradeCostMode == 'absolute':
            oneTradeCost = realTradeCostReference * abs(delta)
            return oneTradeCost
                          
    def trading_process(self,
                        dfToday,
                        tradeCostMode,
                        singleOrBoth,
                        tradeCostReference,
                        freqMuti,
                        shortMaName,
                        longMaName,
                        endTimeLengthBeforeClose=0):
        # 回测交易过程，输出的就是tradingLog
        intiDateTimeMark = dfToday['dateTime'].iloc[0]
        intiRow = self.intialization_of_a_trading_process(intiDateTimeMark)
        tradingRowCol = intiRow.columns
        tradingRecord = []
        tradingRecord.append(intiRow)
        temp = dfToday.copy()
        maRelationColName = 'basicRelativePositionOf' + shortMaName + '_' + longMaName
        temp = temp[['dateTime',
                     'LastPx',
                     maRelationColName]]
        temp= temp.dropna()
        for i in range(len(temp)-endTimeLengthBeforeClose):
            if i != len(temp)-endTimeLengthBeforeClose-1:
                lastRecord = tradingRecord[-1]
                dateTimeMark = temp['dateTime'].iloc[i]
                px = temp['LastPx'].iloc[i]
                delta = temp[maRelationColName].iloc[i] - lastRecord['position'].iloc[-1]
                position = lastRecord['position'].iloc[-1] + delta
                cash = lastRecord['cash'].iloc[-1] - delta*px
                tradeCost = self.oneTradeCostGenerater(tradeCostMode,
                                                       singleOrBoth, 
                                                       tradeCostReference, 
                                                       px, 
                                                       delta)
                cumTradeCost = lastRecord['cumTradeCost'].iloc[-1] + tradeCost
                netValue = px*position + cash - cumTradeCost
                newRow = pd.DataFrame([[dateTimeMark,
                                        px,
                                        position,
                                        cash,
                                        tradeCost,
                                        cumTradeCost,
                                        delta,
                                        netValue]],
                                       columns=tradingRowCol)
                tradingRecord.append(newRow)
                
            else:
                lastRecord = tradingRecord[-1]
                dateTimeMark = temp['dateTime'].iloc[-endTimeLengthBeforeClose-1]
                px = temp['LastPx'].iloc[-endTimeLengthBeforeClose-1]
                delta = -lastRecord['position'].iloc[-1]
                position = 0
                cash = lastRecord['cash'].iloc[-1] - delta*px 
                tradeCost = self.oneTradeCostGenerater(tradeCostMode,
                                                       singleOrBoth, 
                                                       tradeCostReference, 
                                                       px, 
                                                       delta)
                
                cumTradeCost = lastRecord['cumTradeCost'].iloc[-1] + tradeCost
                netValue = px*position + cash - cumTradeCost
                newRow = pd.DataFrame([[dateTimeMark,
                                        px,
                                        position,
                                        cash,
                                        tradeCost,
                                        cumTradeCost,
                                        delta,
                                        netValue]],
                                       columns=tradingRowCol)
                tradingRecord.append(newRow)
        tradingRecord = np.vstack(tradingRecord)
        tradingRecord = pd.DataFrame(tradingRecord,columns=tradingRowCol)    
        return tradingRecord 
    
    def achieve_tradingLog(self,
                           dfYesterdayFileAddress,
                           dfTodayFileAddress,
                           shortMaName,
                           longMaName,
                           freqMuti,
                           tradeCostMode,
                           singleOrBoth,
                           tradeCostReference,
                           tradingLogSaveFolder):
        # 回测交易并储存tradingLog，也是这个类的主函数
        try:
            start = time.time()
            dfYesterday = pd.read_csv(dfYesterdayFileAddress)
            dfToday = pd.read_csv(dfTodayFileAddress)
            dfToday = self.calculate_ma_pairs_for_trading_day(dfYesterday,
                                                              dfToday,
                                                              shortMaName,
                                                              longMaName,
                                                              freqMuti)
            tradingLog = self.trading_process(dfToday, 
                                              tradeCostMode, 
                                              singleOrBoth, 
                                              tradeCostReference, 
                                              freqMuti,
                                              shortMaName,
                                              longMaName)
            # shortMaCol = [0] + [i for i in dfToday[shortMaName]]
            # longMaCol = [0] + [i for i in dfToday[longMaName]]
            # tradingLog[shortMaName] = shortMaCol
            # tradingLog[longMaName] = longMaCol
            tradingDate = dfTodayFileAddress.split('/')[-1]
            tradingDate = re.findall(r"\d+",tradingDate)[0]
            tradingLog['tradingDate'] = tradingDate
            saveAddressTail = dfTodayFileAddress.split('/')[-1]
            tradingLogSaveAddress = tradingLogSaveFolder + saveAddressTail
            tradingLog.to_csv(tradingLogSaveAddress)
            end = time.time()
            period = int(end - start)
            print('It takes ' + str(period) + ' to achieve and save tradingLog of ' + dfTodayFileAddress)
            message = 0
            return message
        except Exception as e:
            message = dfTodayFileAddress + ' with ' + str(e)
            print(message)
            return message

class CTA_Trading_Rule_With_Bolling(Basic_CTA_Trading_Rule):
    
    def calculate_bolling(self,
                          bollingLength,
                          freqMuti,
                          series):
        realFreq = bollingLength*freqMuti
        mb = series.rolling(realFreq).mean()
        md = series.rolling(realFreq).std()
        up = mb + 2*md
        dn = mb - 2*md
        return mb, up, dn
    
    def calculate_bolling_for_trading_day(self, 
                                          dfYesterday, 
                                          dfToday, 
                                          shortMaName,
                                          longMaName,
                                          bollingLength, 
                                          freqMuti):
        tempYesterday = dfYesterday.copy()
        shortMaLength = re.findall(r"\d+",shortMaName)[0]
        shortMaType = shortMaName[:-(len(shortMaLength)+1)]
        shortMaLength = int(shortMaLength)
        tempYesterday[shortMaName] = self.calculate_ma(shortMaType,
                                                       shortMaLength,
                                                       freqMuti,
                                                       tempYesterday['LastPx'])
        longMaLength = re.findall(r"\d+",longMaName)[0]
        longMaType = longMaName[:-(len(longMaLength)+1)]
        longMaLength = int(longMaLength)
        tempYesterday[longMaName] = self.calculate_ma(longMaType,
                                                      longMaLength,
                                                      freqMuti,
                                                      tempYesterday['LastPx'])
        multiplier = self.sew_up(tempYesterday,
                                 dfToday,
                                 shortMaName,
                                 longMaName)
        yesterdayReference = tempYesterday['LastPx']*multiplier
        tempToday = dfToday.copy()
        wholeSeries = pd.concat([yesterdayReference,tempToday['LastPx']]) 
        bollingMB, bollingUP, bollingDN = self.calculate_bolling(bollingLength, 
                                                                 freqMuti, 
                                                                 wholeSeries)
        tempToday['bollingMB'] = bollingMB[len(tempYesterday):]
        tempToday['bollingUP'] = bollingUP[len(tempYesterday):]
        tempToday['bollingDN'] = bollingDN[len(tempYesterday):]
        return tempToday
    
    def px_relation_with_bollingMB(self,
                                   px,
                                   bollingMB):
        if px >= bollingMB:
            return 1
        else:
            return -1
    
    def delta_filter_bollingMB(self,
                               rawDelta,
                               pxRelationWithBollingMB):
        if rawDelta*pxRelationWithBollingMB > 0:
            return rawDelta
        else:
            return 0
    
    def trading_process(self,
                        dfToday,
                        tradeCostMode,
                        singleOrBoth,
                        tradeCostReference,
                        freqMuti,
                        shortMaName,
                        longMaName):
        intiDateTimeMark = dfToday['dateTime'].iloc[0]
        intiRow = self.intialization_of_a_trading_process(intiDateTimeMark)
        tradingRowCol = intiRow.columns
        tradingRecord = []
        tradingRecord.append(intiRow)
        temp = dfToday.copy()
        maRelationColName = 'basicRelativePositionOf' + shortMaName + '_' + longMaName
        temp = temp[['dateTime',
                     'LastPx',
                     maRelationColName,
                     'bollingMB',
                     'bollingUP',
                     'bollingDN']]
        temp= temp.dropna()
        for i in range(len(temp)):
            if i != len(temp)-1:
                lastRecord = tradingRecord[-1]
                dateTimeMark = temp['dateTime'].iloc[i]
                px = temp['LastPx'].iloc[i]
                rawDelta = temp[maRelationColName].iloc[i] - lastRecord['position'].iloc[-1]
                pxRelationWithBollingMB = self.px_relation_with_bollingMB(px, 
                                                                          temp['bollingMB'].iloc[i])
                delta = self.delta_filter_bollingMB(rawDelta, 
                                                    pxRelationWithBollingMB)
                position = lastRecord['position'].iloc[-1] + delta
                cash = lastRecord['cash'].iloc[-1] - delta*px
                tradeCost = self.oneTradeCostGenerater(tradeCostMode,
                                                       singleOrBoth, 
                                                       tradeCostReference, 
                                                       px, 
                                                       delta)
                cumTradeCost = lastRecord['cumTradeCost'].iloc[-1] + tradeCost
                netValue = px*position + cash - cumTradeCost
                newRow = pd.DataFrame([[dateTimeMark,
                                        px,
                                        position,
                                        cash,
                                        tradeCost,
                                        cumTradeCost,
                                        delta,
                                        netValue]],
                                       columns=tradingRowCol)
                tradingRecord.append(newRow)
                
            else:
                lastRecord = tradingRecord[-1]
                dateTimeMark = temp['dateTime'].iloc[-1]
                px = temp['LastPx'].iloc[-1]
                delta = -lastRecord['position'].iloc[-1]
                position = 0
                cash = lastRecord['cash'].iloc[-1] - delta*px 
                tradeCost = self.oneTradeCostGenerater(tradeCostMode,
                                                       singleOrBoth, 
                                                       tradeCostReference, 
                                                       px, 
                                                       delta)
                
                cumTradeCost = lastRecord['cumTradeCost'].iloc[-1] + tradeCost
                netValue = px*position + cash - cumTradeCost
                newRow = pd.DataFrame([[dateTimeMark,
                                        px,
                                        position,
                                        cash,
                                        tradeCost,
                                        cumTradeCost,
                                        delta,
                                        netValue]],
                                       columns=tradingRowCol)
                tradingRecord.append(newRow)
        tradingRecord = np.vstack(tradingRecord)
        tradingRecord = pd.DataFrame(tradingRecord,columns=tradingRowCol)    
        return tradingRecord 
    
    def achieve_tradingLog(self,
                           dfYesterdayFileAddress, 
                           dfTodayFileAddress, 
                           shortMaName, 
                           longMaName, 
                           bollingLength,
                           freqMuti, 
                           tradeCostMode, 
                           singleOrBoth, 
                           tradeCostReference, 
                           tradingLogSaveFolder):
        try:
            start = time.time()
            dfYesterday = pd.read_csv(dfYesterdayFileAddress)
            dfToday = pd.read_csv(dfTodayFileAddress)
            dfToday = self.calculate_ma_pairs_for_trading_day(dfYesterday,
                                                              dfToday,
                                                              shortMaName,
                                                              longMaName,
                                                              freqMuti)
            dfToday = self.calculate_bolling_for_trading_day(dfYesterday, 
                                                             dfToday, 
                                                             shortMaName,
                                                             longMaName,
                                                             bollingLength, 
                                                             freqMuti)
            tradingLog = self.trading_process(dfToday, 
                                              tradeCostMode, 
                                              singleOrBoth, 
                                              tradeCostReference, 
                                              freqMuti,
                                              shortMaName,
                                              longMaName)
            shortMaCol = [0] + [i for i in dfToday[shortMaName]]
            longMaCol = [0] + [i for i in dfToday[longMaName]]
            tradingLog[shortMaName] = shortMaCol
            tradingLog[longMaName] = longMaCol
            bollingMBCol = [0] + [i for i in dfToday['bollingMB']]
            bollingUPCol = [0] + [i for i in dfToday['bollingUP']]
            bollingDNCol = [0] + [i for i in dfToday['bollingDN']]
            tradingLog['bollingMB'] = bollingMBCol
            tradingLog['bollingUP'] = bollingUPCol
            tradingLog['bollingDN'] = bollingDNCol
            tradingDate = dfTodayFileAddress.split('/')[-1]
            tradingDate = re.findall(r"\d+",tradingDate)[0]
            tradingLog['tradingDate'] = tradingDate
            saveAddressTail = dfTodayFileAddress.split('/')[-1]
            tradingLogSaveAddress = tradingLogSaveFolder + saveAddressTail
            tradingLog.to_csv(tradingLogSaveAddress)
            end = time.time()
            period = int(end - start)
            print('It takes ' + str(period) + ' to achieve and save tradingLog of ' + dfTodayFileAddress)
            message = 0
            return message
        except Exception as e:
            message = dfTodayFileAddress + ' with ' + str(e)
            print(message)
            return message        
    

class Quick_Enter_Slow_Out(CTA_Trading_Rule_With_Bolling):
    
    def intialization_of_a_trading_process(self,
                                           intiDateTimeMark):
        dateTimeMark = intiDateTimeMark
        px = 0
        position = 0
        cash = 0 
        tradeCost = 0
        cumTradeCost = 0
        delta = 0
        netValue = 0
        slowLock = [0]
        intiRow = pd.DataFrame([[dateTimeMark,
                                 px,
                                 position,
                                 cash,
                                 tradeCost,
                                 cumTradeCost,
                                 delta,
                                 netValue,
                                 slowLock[0]]],
                               columns=['dateTimeMark',
                                        'px',
                                        'position',
                                        'cash',
                                        'tradeCost',
                                        'cumTradeCost',
                                        'delta',
                                        'netValue',
                                        'slowLock'])
        return intiRow 
    
    def achieve_tradingLog(self,
                           dfYesterdayFileAddress, 
                           dfTodayFileAddress, 
                           shortMaName, 
                           longMaName, 
                           bollingLength,
                           slowShortMaName,
                           slowLongMaName,
                           freqMuti, 
                           tradeCostMode, 
                           singleOrBoth, 
                           tradeCostReference, 
                           tradingLogSaveFolder):
        try:
            start = time.time()
            dfYesterday = pd.read_csv(dfYesterdayFileAddress)
            dfToday = pd.read_csv(dfTodayFileAddress)
            dfToday = self.calculate_ma_pairs_for_trading_day(dfYesterday,
                                                              dfToday,
                                                              shortMaName,
                                                              longMaName,
                                                              freqMuti)
            dfToday = self.calculate_bolling_for_trading_day(dfYesterday, 
                                                             dfToday, 
                                                             shortMaName,
                                                             longMaName,
                                                             bollingLength, 
                                                             freqMuti)
            dfToday = self.calculate_ma_pairs_for_trading_day(dfYesterday, 
                                                              dfToday, 
                                                              slowShortMaName, 
                                                              slowLongMaName, 
                                                              freqMuti)
            tradingLog = self.trading_process(dfToday, 
                                              tradeCostMode, 
                                              singleOrBoth, 
                                              tradeCostReference, 
                                              freqMuti,
                                              shortMaName,
                                              longMaName,
                                              slowShortMaName,
                                              slowLongMaName)
            shortMaCol = [0] + [i for i in dfToday[shortMaName]]
            longMaCol = [0] + [i for i in dfToday[longMaName]]
            tradingLog[shortMaName] = shortMaCol
            tradingLog[longMaName] = longMaCol
            bollingMBCol = [0] + [i for i in dfToday['bollingMB']]
            bollingUPCol = [0] + [i for i in dfToday['bollingUP']]
            bollingDNCol = [0] + [i for i in dfToday['bollingDN']]
            tradingLog['bollingMB'] = bollingMBCol
            tradingLog['bollingUP'] = bollingUPCol
            tradingLog['bollingDN'] = bollingDNCol
            tradingDate = dfTodayFileAddress.split('/')[-1]
            tradingDate = re.findall(r"\d+",tradingDate)[0]
            tradingLog['tradingDate'] = tradingDate
            saveAddressTail = dfTodayFileAddress.split('/')[-1]
            tradingLogSaveAddress = tradingLogSaveFolder + saveAddressTail
            tradingLog.to_csv(tradingLogSaveAddress)
            end = time.time()
            period = int(end - start)
            print('It takes ' + str(period) + ' to achieve and save tradingLog of ' + dfTodayFileAddress)
            message = 0
            return message
        except Exception as e:
            message = dfTodayFileAddress + ' with ' + str(e)
            print(message)
            return message 
    
    def trading_process(self,
                        dfToday,
                        tradeCostMode,
                        singleOrBoth,
                        tradeCostReference,
                        freqMuti,
                        shortMaName,
                        longMaName,
                        slowShortMaName,
                        slowLongMaName):
        intiDateTimeMark = dfToday['dateTime'].iloc[0]
        intiRow = self.intialization_of_a_trading_process(intiDateTimeMark)
        tradingRowCol = intiRow.columns
        tradingRecord = []
        tradingRecord.append(intiRow)
        temp = dfToday.copy()
        maRelationColName = 'basicRelativePositionOf' + shortMaName + '_' + longMaName
        slowMaRelationColName = 'basicRelativePositionOf' + slowShortMaName + '_' + slowLongMaName
        temp = temp[['dateTime',
                     'LastPx',
                     maRelationColName,
                     'bollingMB',
                     'bollingUP',
                     'bollingDN',
                     slowMaRelationColName]]
        temp= temp.dropna()
        slowLock = [0]
        for i in range(len(temp)):
            if i != len(temp) - 1:

                lastRecord = tradingRecord[-1]
                if lastRecord['position'].iloc[-1] == 0:
                    dateTimeMark = temp['dateTime'].iloc[i]
                    px = temp['LastPx'].iloc[i]
                    rawDelta = temp[maRelationColName].iloc[i] - lastRecord['position'].iloc[-1]
                    pxRelationWithBollingMB = self.px_relation_with_bollingMB(px, 
                                                                              temp['bollingMB'].iloc[i])
                    delta = self.delta_filter_bollingMB(rawDelta, 
                                                        pxRelationWithBollingMB)
                    position = lastRecord['position'].iloc[-1] + delta
                    if position == temp[slowMaRelationColName].iloc[i]:
                        slowLock[0] = position
                    cash = lastRecord['cash'].iloc[-1] - delta*px
                    tradeCost = self.oneTradeCostGenerater(tradeCostMode,
                                                           singleOrBoth, 
                                                           tradeCostReference, 
                                                           px, 
                                                           delta)
                    cumTradeCost = lastRecord['cumTradeCost'].iloc[-1] + tradeCost
                    netValue = px*position + cash - cumTradeCost
                    newRow = pd.DataFrame([[dateTimeMark,
                                            px,
                                            position,
                                            cash,
                                            tradeCost,
                                            cumTradeCost,
                                            delta,
                                            netValue,
                                            slowLock[0]]],
                                           columns=tradingRowCol)
                    tradingRecord.append(newRow)
                else:
                    dateTimeMark = temp['dateTime'].iloc[i]
                    px = temp['LastPx'].iloc[i]
                    if slowLock[0] != 0:
                        slowDelta = temp[slowMaRelationColName].iloc[i] - lastRecord['position'].iloc[-1]  
                        if slowDelta == 0:
                            position = lastRecord['position'].iloc[-1] + slowDelta
                            cash = lastRecord['cash'].iloc[-1] - slowDelta*px
                            tradeCost = self.oneTradeCostGenerater(tradeCostMode,
                                                                   singleOrBoth, 
                                                                   tradeCostReference, 
                                                                   px, 
                                                                   slowDelta)
                            cumTradeCost = lastRecord['cumTradeCost'].iloc[-1] + tradeCost
                            netValue = px*position + cash - cumTradeCost
                            newRow = pd.DataFrame([[dateTimeMark,
                                                    px,
                                                    position,
                                                    cash,
                                                    tradeCost,
                                                    cumTradeCost,
                                                    slowDelta,
                                                    netValue,
                                                    slowLock[0]]],
                                                   columns=tradingRowCol)
                            tradingRecord.append(newRow) 
                        else:
                            delta1 = -lastRecord['position'].iloc[-1]
                            slowLock[0] = 0
                            delta2 = temp[maRelationColName].iloc[i] - lastRecord['position'].iloc[-1]
                            pxRelationWithBollingMB = self.px_relation_with_bollingMB(px, 
                                                                                      temp['bollingMB'].iloc[i])
                            delta2 = self.delta_filter_bollingMB(delta2, 
                                                                 pxRelationWithBollingMB)
                            if delta1*delta2 <= 0:
                                finalDelta = delta1
                                slowLock[0] = 0
                            else:
                                finalDelta = delta2
                                slowLock[0] = 1
                            position = lastRecord['position'].iloc[-1] + finalDelta
                            cash = lastRecord['cash'].iloc[-1] - finalDelta*px
                            tradeCost = self.oneTradeCostGenerater(tradeCostMode,
                                                                   singleOrBoth, 
                                                                   tradeCostReference, 
                                                                   px, 
                                                                   finalDelta)
                            cumTradeCost = lastRecord['cumTradeCost'].iloc[-1] + tradeCost
                            netValue = px*position + cash - cumTradeCost
                            newRow = pd.DataFrame([[dateTimeMark,
                                                    px,
                                                    position,
                                                    cash,
                                                    tradeCost,
                                                    cumTradeCost,
                                                    finalDelta,
                                                    netValue,
                                                    slowLock[0]]],
                                                   columns=tradingRowCol)
                            tradingRecord.append(newRow)   
                    else:
                        dateTimeMark = temp['dateTime'].iloc[i]
                        px = temp['LastPx'].iloc[i]
                        if lastRecord['position'].iloc[-1] == temp[slowMaRelationColName].iloc[i]:
                            slowLock[0] = position
                            slowDelta = 0
                            position = lastRecord['position'].iloc[-1] + slowDelta
                            cash = lastRecord['cash'].iloc[-1] - slowDelta*px
                            tradeCost = self.oneTradeCostGenerater(tradeCostMode,
                                                                   singleOrBoth, 
                                                                   tradeCostReference, 
                                                                   px, 
                                                                   slowDelta)
                            cumTradeCost = lastRecord['cumTradeCost'].iloc[-1] + tradeCost
                            netValue = px*position + cash - cumTradeCost
                            newRow = pd.DataFrame([[dateTimeMark,
                                                    px,
                                                    position,
                                                    cash,
                                                    tradeCost,
                                                    cumTradeCost,
                                                    slowDelta,
                                                    netValue,
                                                    slowLock[0]]],
                                                   columns=tradingRowCol)
                            tradingRecord.append(newRow)
                        else:
                            slowLock[0] = 0
                            rawDelta = temp[maRelationColName].iloc[i] - lastRecord['position'].iloc[-1]
                            pxRelationWithBollingMB = self.px_relation_with_bollingMB(px, 
                                                                                      temp['bollingMB'].iloc[i])
                            delta = self.delta_filter_bollingMB(rawDelta, 
                                                                pxRelationWithBollingMB)
                            position = lastRecord['position'].iloc[-1] + delta
                            if position == temp[slowMaRelationColName].iloc[i]:
                                slowLock[0] = position
                            cash = lastRecord['cash'].iloc[-1] - delta*px
                            tradeCost = self.oneTradeCostGenerater(tradeCostMode,
                                                                   singleOrBoth, 
                                                                   tradeCostReference, 
                                                                   px, 
                                                                   delta)
                            cumTradeCost = lastRecord['cumTradeCost'].iloc[-1] + tradeCost
                            netValue = px*position + cash - cumTradeCost
                            newRow = pd.DataFrame([[dateTimeMark,
                                                    px,
                                                    position,
                                                    cash,
                                                    tradeCost,
                                                    cumTradeCost,
                                                    delta,
                                                    netValue,
                                                    slowLock[0]]],
                                                   columns=tradingRowCol)
                            tradingRecord.append(newRow)                            
            else:
                lastRecord = tradingRecord[-1]
                dateTimeMark = temp['dateTime'].iloc[-1]
                px = temp['LastPx'].iloc[-1]
                delta = -lastRecord['position'].iloc[-1]
                position = 0
                cash = lastRecord['cash'].iloc[-1] - delta*px 
                tradeCost = self.oneTradeCostGenerater(tradeCostMode,
                                                       singleOrBoth, 
                                                       tradeCostReference, 
                                                       px, 
                                                       delta)
                
                cumTradeCost = lastRecord['cumTradeCost'].iloc[-1] + tradeCost
                netValue = px*position + cash - cumTradeCost
                newRow = pd.DataFrame([[dateTimeMark,
                                        px,
                                        position,
                                        cash,
                                        tradeCost,
                                        cumTradeCost,
                                        delta,
                                        netValue,
                                        slowLock[0]]],
                                       columns=tradingRowCol)
                tradingRecord.append(newRow)
        tradingRecord = np.vstack(tradingRecord)
        tradingRecord = pd.DataFrame(tradingRecord,columns=tradingRowCol)    
        return tradingRecord
    
    

    
if __name__ == '__main__':
    
    mainFileFolder =  'F:/main/rb_min/'
    shortMaName = 'MA25m'
    longMaName = 'MA50m'
    bollingLength = 100
    slowShortMaName = 'MA25m'
    slowLongMaName = 'MA100m'
    freqMuti = 1
    tradeCostMode = 'relative'
    singleOrBoth = 'both'
    tradeCostReference = 0.0001
    tradingLogSaveFolder = 'F:/trading_log/rb/MA25m-MA50m-BollingMB_(MA25m-MA100m)_new/'
    argSummarySaveAddress = 'E:/machineLearning/ma_enhanced_project/stat_facts/trading_trigger/fiveTenMA_bolling/rb/MA25m-MA50m-BollingMB_(MA25m-MA100m)_new/argSummary(MA25m-MA50m-BollingMB_(MA25m-MA100m)_new).xlsx'
    errorMessageSaveAddress = 'E:/machineLearning/ma_enhanced_project/stat_facts/trading_trigger/fiveTenMA_bolling/rb/MA25m-MA50m-BollingMB_(MA25m-MA100m)_new/errorMessageFromAchieveTradingLogProcess20230322.xlsx'
    endPointNumber = 1
 

    # fixedArguementsList = [shortMaName,
    #                        longMaName,
    #                        freqMuti,
    #                        tradeCostMode,
    #                        singleOrBoth,
    #                        tradeCostReference,
    #                        tradingLogSaveFolder]
       
    fixedArguementsList = [shortMaName,
                           longMaName,
                           bollingLength,
                           slowShortMaName,
                           slowLongMaName,
                           freqMuti,
                           tradeCostMode,
                           singleOrBoth,
                           tradeCostReference,
                           tradingLogSaveFolder]
    
    argSummary = pd.DataFrame([fixedArguementsList])
    argSummaryColumns = ['shortMaName',
                         'longMaName',
                         'bollingLength',
                         'slowShortMaName',
                         'slowLongMaName',
                         'freqMuti',
                         'tradeCostMode',
                         'singleOrBoth',
                         'tradeCostReference',
                         'tradingLogSaveFolder']
    argSummary.columns = argSummaryColumns
    argSummary = argSummary.T
    print(argSummary)
    userInput = input('Please Check the args.')
    if userInput == 'y':
        argSummary.to_excel(argSummarySaveAddress)
    ct = Common_Tools()
    # ctrwb = CTA_Trading_Rule_With_Bolling()
    # bctr = Basic_CTA_Trading_Rule()
    qeso = Quick_Enter_Slow_Out()

    mainFilesAddress = ct.oriData_arranged_by_date(mainFileFolder)
    errorMessageRecord = ct.mutiprocess_engine_serverally_save(qeso.achieve_tradingLog,
                                                                mainFilesAddress['address'], 
                                                                endPointNumber, 
                                                                fixedArguementsList)
    errorMessageRecord.to_excel(errorMessageSaveAddress)
    
    # test = qeso.achieve_tradingLog(mainFilesAddress['address'].iloc[0], 
    #                                mainFilesAddress['address'].iloc[1], 
    #                                shortMaName, 
    #                                longMaName, 
    #                                bollingLength, 
    #                                slowShortMaName, 
    #                                slowLongMaName, 
    #                                freqMuti, 
    #                                tradeCostMode, 
    #                                singleOrBoth, 
    #                                tradeCostReference, 
    #                                tradingLogSaveFolder)
    


    


    
    
    

    



        
    
    
    
    