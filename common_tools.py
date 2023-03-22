# -*- coding: utf-8 -*-
"""
Created on Wed Jan 18 15:04:01 2023

@author: South
"""

import pandas as pd
import os
import datetime as dt
from multiprocessing import Pool
import multiprocessing
import numpy as np 

class Common_Tools():
    
    def __init__(self):
        pass
    
    def oriData_arranged_by_date(self,
                                 oriDataSaveFolder):
        oriDataSaveAddress = os.listdir(oriDataSaveFolder)
        oriDataSaveAddress = pd.DataFrame(oriDataSaveAddress,
                                          columns=['address'])
        oriDataSaveAddress['date'] = oriDataSaveAddress['address'].apply(lambda x: x[:8])
        oriDataSaveAddress['date'] = oriDataSaveAddress['date'].apply(lambda x: dt.date(int(x[:4]),int(x[4:6]),int(x[6:])))
        oriDataSaveAddress = oriDataSaveAddress.sort_values(['date'])
        oriDataSaveAddress['address'] = oriDataSaveAddress['address'].apply(lambda x: oriDataSaveFolder + x )
        return oriDataSaveAddress
    
    def mutiprocess_engine_concentrative_save(self,
                                              function,
                                              iterableDataAddress,
                                              endPointNumber,
                                              fixedArguementsList):
        cpuCoreN = multiprocessing.cpu_count() - 1 
        p = Pool(cpuCoreN)
        result = []
        for i in range(0,len(iterableDataAddress) - endPointNumber):
            arguementsList = [iterableDataAddress.iloc[i-j] for j in range(0,endPointNumber+1)]
            arguementsList = arguementsList + fixedArguementsList
            args = tuple(arguementsList)
            temp = p.apply_async(function,args=args)
            result.append(temp)
        p.close()
        p.join() 
        result = [i.get() for i in result]
        valueResult = [i[0] for i in result]
        valueResultCol = valueResult[0].columns
        valueResult = np.vstack(valueResult)
        valueResult =pd.DataFrame(valueResult)
        valueResult.columns = valueResultCol
        errorMessage = [i[1] for i in result]
        errorMessage = np.vstack(errorMessage)
        errorMessage = pd.DataFrame(errorMessage)
        return valueResult,errorMessage
    
    def mutiprocess_engine_serverally_save(self,
                                           function,
                                           iterableDataAddress,
                                           endPointNumber,
                                           fixedArguementsList):
        cpuCoreN = multiprocessing.cpu_count() - 1 
        p = Pool(cpuCoreN)
        errorMessageRecord = []
        for i in range(endPointNumber,len(iterableDataAddress)):
            arguementsList = [iterableDataAddress.iloc[i-j] for j in range(endPointNumber,-1,-1)]
            arguementsList = arguementsList + fixedArguementsList
            args = tuple(arguementsList)
            errorMessage = p.apply_async(function,args=args)
            errorMessageRecord.append(errorMessage)
        p.close()
        p.join() 
        errorMessageRecord = [i.get() for i in errorMessageRecord]
        errorMessageRecord = np.vstack(errorMessageRecord)
        errorMessageRecord = pd.DataFrame(errorMessageRecord)
        return errorMessageRecord
    
    
        
if __name__ == '__main__':
    
    oriDataSaveFolder = 'F:/main/sc/'
    endPointNumber = 0
    ct = Common_Tools()
    oriDataSaveAddress = ct.oriData_arranged_by_date(oriDataSaveFolder)
    iterableDataAddress = oriDataSaveAddress['address']
    a = [1,2,3]
    b = tuple(a)





    
        