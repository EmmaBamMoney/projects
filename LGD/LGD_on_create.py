#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep  5 16:30:05 2019

@author: stan
"""

'''This projetc calculates the Loss Given Default
of the assets on BAM's platform. '''

from selenium import webdriver
from bs4 import BeautifulSoup
import requests
import re
import datetime
import calendar
import pandas as pd
import numpy as np
import datetime
import time
import sys



def mat_judge(i):
    ''' Input should be a string. '''
    temp=str(i).split(' ')
    if len(temp)>2:
        res=temp[1:]
        res[0]=str(list(calendar.month_abbr).index(res[0]))
        res='/'.join(res)
        return datetime.datetime.strptime(res,'%M/%d/%Y')
    else:
        try:
            return datetime.datetime.strptime(str(i),'%M/%d/%Y')
        except:
            return



def time_in_default(x):
    '''The input should be Timestamp data.'''
    '''Ideally, we will use dateOfArrears to calculate the
       time in default.'''
    '''the result is in years'''
    
    d1 = datetime.date.today()
    d2 = x
    time_in_default = (d1-d2).days/365
    return time_in_default




def individual_recovery_rate(asset):
    '''Calculate the recovery rate of each asset'''

    # BDA is
    # BDA should be updated every yea
    BDA = 0.01

    # For tradeClaim assets
    if asset.loc[ "instrumentType"] == "TradeClaim":
        alpha = 35
        beta1 = -5
        beta2 = -10
        beta3 = 10
        beta4 = -500

        # Calculate Time_in_default
        if asset.loc["dateOfArrears"] is not None:
            dateOfArrears = asset['dateOfArrears'].to_pydatetime().date()
            TID = time_in_default(dateOfArrears)
        elif not(mat_judge(asset.loc['maturity']) is not None or mat_judge(asset.loc['maturity'])== ""):
            maturity = mat_judge(asset.loc['maturity']).date()
            if (maturity - datetime.date.today()).days < 0:
                TID = time_in_default(maturity)
            else:
                TID = 1

        dum_secure = asset.loc["collateral"] == "Yes"
        dum_industry = asset.loc["sectorType"] == "utilities"
        RR = alpha + beta1* TID + beta2* dum_secure + beta3 * dum_industry + beta4 * BDA
        RR = max(0, min(RR, 100))
        return RR
    # For other assets:
    else:
        alpha = 40
        beta = 35
        beta1 = 20
        beta2 = 13
        beta3 = -10
        beta4 = -5
        beta5 = -10
        beta6 = 25
        beta7 = -500

        dum_Loan = asset["instrumentType"] == "Loan"
        dum_industry = asset["sectorType"] == "utilities"
        dum_senior = asset["debtSeniority"] == "Senior secured" or asset["debtSeniority"] == "Senior unsecured"
        dum_subordinated = asset["debtSeniority"] == "Senior Subordinated"
        dum_secure = asset.loc["debtSeniority"] == "Senior secured" or asset["collateral"] == "Yes"
        dum_distress = asset.loc["assetClass"] == "distressed" or asset["debtSeniority"] == "deepDistressed"

        # Calculate Time_in_default
        if not (asset["dateOfArrears"] is None or pd.isnull(asset["dateOfArrears"])):
            dateOfArrears = asset['dateOfArrears'].to_pydatetime().date()
            TID = time_in_default(dateOfArrears)
        elif not(mat_judge(asset['maturity']) is None or mat_judge(asset.loc['maturity'])== ""):
            maturity = mat_judge(asset['maturity']).date()
            if (maturity - datetime.date.today()).days < 0:
                TID = time_in_default(maturity)
            else:
                TID = 1
        else:
            TID = 1

        RR = alpha + beta* dum_Loan+ beta1 * dum_senior + beta2 * dum_subordinated+ beta3 * dum_secure + beta4 *TID \
             + beta5 * dum_distress + beta6 * dum_industry + beta7* BDA
        RR = max(0,min(RR,100))

        return RR




def portfolio_loss_given_default(IDs, notionals, df):
        df = df.reset_index().drop(['index'], axis=1)
        df.index = df['id']
        ratio = np.array([notional / sum(notionals) for notional in notionals])
        LGD=[]
        for ID in IDs:
            LGD.append(df.loc[ID,"LGD"]/100)
        portfolio_LGD = np.dot(ratio, LGD)
        return portfolio_LGD




import pymongo
from bson.objectid import ObjectId

# asset_id = sys.argv[1]

asset_id = "5bb7eb2ae815641f34164152"

client = pymongo.MongoClient('localhost', 27017)
mydb = client["bam"]
mycol = mydb["assets"]
df = pd.DataFrame(list(mycol.find()))


number = 100 - individual_recovery_rate(df[df["_id"] == ObjectId(asset_id)].iloc[0])

renew = {}
renew['LGD'] = number

print(number)
# mycol.update_one({"_id": ObjectId(asset_id)}, {"$set": renew})
