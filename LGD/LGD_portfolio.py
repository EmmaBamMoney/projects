'''This projetc calculates the Loss Given Default
of the assets on BAM's platform. '''

import datetime
import calendar
import pandas as pd
import numpy as np
import pymongo
from bson.objectid import ObjectId
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
            return datetime.datetime.strptime(i,'%M/%d/%Y')
        except:
            return



def time_in_default(x):
    '''The input should be Timestamp data.'''
    '''Ideally, we will use dateOfArrears to calculate the
       time in default.'''
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
        if asset.loc[ "dateOfArrears"] is not None:
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




def portfolio_loss_given_default(portfolioID, df_portfolio, df):
        '''portfolioID is a string'''
        "'Return Portfolio_LGD as a percentage'"
        portfolio = df_portfolio[df_portfolio["name"] == portfolioID]
        notionals = np.array([item for sublist in portfolio["notionals"] for item in sublist])
        IDs = portfolio["assets"]
        df = df.reset_index().drop(['index'], axis=1)
        df.index = df['_id']
        ratio = np.array([notional / sum(notionals) for notional in notionals])
        LGD=[]
        for ID in IDs:
            LGD.append(df.loc[ID,"LGD"]/100)
        portfolio_LGD = np.dot(ratio, LGD[0])
        return portfolio_LGD

client = pymongo.MongoClient('localhost', 27017)
mydb = client["bam"]
mycol = mydb["assets"]
df = pd.DataFrame(list(mycol.find()))

mycol2 = mydb["portfolios"]
df_portfolio = pd.DataFrame(list(mycol2.find()))

portfolioID = sys.argv[1]
#portfolioID = df_portfolio["name"][1]

try:
    portfolio_LGD = portfolio_loss_given_default(portfolioID, df_portfolio, df)
    renew = {}
    renew['LGD'] = portfolio_LGD
    mycol2.update_many({"name": portfolioID}, {"$set": renew})
    
    print(portfolio_LGD)
except:
    print("portfolio not found")
