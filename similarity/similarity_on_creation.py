#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep  5 12:00:48 2019

@author: stan
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 13 16:18:13 2019

@author: peiluzhang
"""

import pandas as pd
import numpy as np
import math
import pickle
import os
from pymongo import MongoClient
from bson.objectid import ObjectId
import math
import sys

client = MongoClient('localhost', 27017)
mydb = client["bam"]
mycol = mydb["assets"]
df = pd.DataFrame(list(mycol.find()))

#os.chdir('/Users/peiluzhang/Documents/BAM.money/project4-asset-similarity')

#df = pd.read_excel('assets-06-07.xlsx')

# extract useful fields for similarity analysis
df_asset = df[['_id','title','pitch','currency','chapter11','assetClass','collateral','negotiationTerms','totalDebt','marketCap','debtSeniority','instrumentType','notional','maturity','coupon','creditType','debtor','creditor','sectorType']]

#list1 = [float(i.replace("$", "").replace(",", "").replace('BRL', '').replace('‚Ç¨', '').replace('¬£', '').replace('NT', '').replace('¬•', '').replace('£', '').replace('€', '').replace('¥', '')) for i in df_asset['notional']]
list1 = []
for i in df_asset['notional']:
    try:
        if type(i) == np.int64:
            list1.append(i)
        else:
            list1.append(float(i.replace("$", "").replace(",", "").replace('BRL', '').replace('‚Ç¨', '').replace('¬£', '').replace('NT', '').replace('¬•', '').replace('£', '').replace('€', '').replace('¥', '')))
    except:
        list1.append(np.nan)



len(df)
len(list1)

notional_tag = []
for value in list1:
    try:
        #if type(value) == str:
        if value <= np.percentile(list1, 25):
            notional_tag.append('small')
        elif value > np.percentile(list1, 25) and value <= np.percentile(list1, 50):
            notional_tag.append('medium')
        elif value > np.percentile(list1, 50) and value <= np.percentile(list1, 75):
            notional_tag.append('Large')
        else:
            notional_tag.append('SuperLarge')
    except:
        notional_tag.append(np.nan)

matu = []
matu_all = []
for value in df_asset['maturity']:
    try:
        if type(value) == str :
            #matu.append(float(str(value).split("-")[0]))
            matu.append(value[-1])
            #matu_all.append(float(str(value).split("-")[0]))
            matu_all.append(float(str(value).split("-")[0]))
        elif type(value) == int:
            matu.append(value[-1])
            #matu_all.append(float(str(value).split("-")[0]))
            matu_all.append(float(str(value).split("-")[0]))
        else:
            matu_all.append(np.nan)
    except:
        matu_all.append(np.nan)

maturity_tag = []
for value in matu_all:
    try:
        if value <= np.percentile(matu, 30):
            maturity_tag.append('short_term')
        elif value > np.percentile(matu, 30) and value <= np.percentile(matu, 60):
            maturity_tag.append('medium_term')
        elif value > np.percentile(matu, 60):
            maturity_tag.append('long_term')
        else:
            maturity_tag.append(np.nan)
    except:
        maturity_tag.append(np.nan)

coupon = []
for value in df_asset['coupon']:
    try:
        if type(value) == str:
            coupon.append(float(value.split(',')[0][-1])/100)
            index = df_asset.index[df_asset['coupon'] == value].tolist()
            df_asset['coupon'][index] = float(value.split(',')[0][-1])/100
        elif value > 1:
            coupon.append(float(value)/100)
            index = df_asset.index[df_asset['coupon'] == value].tolist()
            df_asset['coupon'][index] =float(value)/100
        elif not math.isnan(value):
            coupon.append(value)
    except:
        coupon.append(np.nan)


coupon_tag = []
for value in df_asset['coupon']:
    try:
        if value <= np.percentile(coupon, 30):
            coupon_tag.append('low_yield')
        elif value > np.percentile(coupon, 30) and value <= np.percentile(coupon, 60):
            coupon_tag.append('medium_yield')
        elif value > np.percentile(coupon, 60):
            coupon_tag.append('high_yield')
        else:
            coupon_tag.append(np.nan)
    except:
        coupon_tag.append(np.nan)

marketcap = []
for value in df_asset['marketCap']:
    try:
        if not math.isnan(value):
            marketcap.append(value)
    except:
        marketcap.append(np.nan)

marketcap_tag = []
for value in df_asset['marketCap']:
    try:
        if value <= np.percentile(marketcap, 30):
            marketcap_tag.append('low_cap')
        elif value > np.percentile(marketcap, 30) and value <= np.percentile(marketcap, 60):
            marketcap_tag.append('medium_cap')
        elif value > np.percentile(marketcap, 60):
            marketcap_tag.append('high_cap')
        else:
            marketcap_tag.append(np.nan)
    except:
        marketcap_tag.append(np.nan)

totaldebt = []
for value in df_asset['totalDebt']:
    try:
        if not math.isnan(value):
            totaldebt.append(value)
    except:
        totaldebt.append(np.nan)

totaldebt_tag = []
for value in df_asset['totalDebt']:
    try:
        if value <= np.percentile(totaldebt, 30):
            totaldebt_tag.append('low_debt')
        elif value > np.percentile(totaldebt, 30) and value <= np.percentile(totaldebt, 60):
            totaldebt_tag.append('medium_debt')
        elif value > np.percentile(totaldebt, 60):
            totaldebt_tag.append('high_debt')
        else:
            totaldebt_tag.append(np.nan)
    except:
        totaldebt_tag.append(np.nan)


df_asset['maturity'] = maturity_tag
df_asset['notional'] = notional_tag
df_asset['coupon'] = coupon_tag
df_asset['marketCap'] = marketcap_tag
df_asset['totalDebt'] = totaldebt_tag

#df_asset['maturity']
#df_asset['notional']
#df_asset['totalDebt']


def similarity(asset, dataset, priority = ['instrumentType','sectorType', 'creditType','debtor', 'creditor','maturity',
                                           'notional','coupon','marketCap','totalDebt','debtSeniority','assetClass','collateral','pitch','currency','chapter11','negotiationTerms',]):
    dis = []
    modes = asset
    #instrument = asset['instrumentType']
    for j in range(len(dataset)):
        #if dataset[j]['instrumentType'] == instrument:
        count = 0
        weight = 20
        for pri in priority:
            if dataset.iloc[j][pri] == modes[pri]:
                count += weight
                weight = weight/2
                if weight <= 1:
                    weight = 1
        dis.append(count)
    idx = list(np.argsort(dis))[::-1]
    ranking =  dataset.iloc[idx[1:4]]['_id']
    dis1 = dis
    maxone = max(dis1)
#    minone = min(dis1)
    dis1 = [i/maxone for i in dis1]
    dis1.sort(reverse = True)
    score = dis1[1:4]
    #names = df_asset['title'].loc[ranking]
    return([list(ranking), score])

asset_id = sys.argv[1]

#asset_id = "5d71571ad9b82b3b0f190d13"
asset = df_asset[df_asset["_id"] == ObjectId(asset_id)]

result_123 = [similarity(asset.iloc[0],df_asset)]

print(result_123)

score_123 = [i[1] for i in result_123]
simi_123 = [i[0] for i in result_123]

#############similarity run on the server##################
#client = MongoClient('localhost', 27017)
#mydb = client["bam"]
#mycol = mydb["assets"]

simi1 = [i[0] for i in simi_123]
simi2 = [i[1] for i in simi_123]
simi3 = [i[2] for i in simi_123]
score1 = [i[0] for i in score_123]
score2 = [i[1] for i in score_123]
score3 = [i[2] for i in score_123]

for i in range(len(simi1)):
    simi_1 = simi1[i]
    simi_2 = simi2[i]
    simi_3 = simi3[i]
    simiscore_1 = score1[i]
    simiscore_2 = score2[i]
    simiscore_3 = score3[i]
    print({"_id":ObjectId(asset_id)}, {"$set": {"similar_1": simi_1,"similar_2":simi_2, "similar_3":simi_3,
                       "simiscore_1": simiscore_1,"simiscore_2": simiscore_3, "simiscore_3": simiscore_3}})
    mycol.update_many({"_id":ObjectId(asset_id)}, {"$set": {"similar_1": simi_1,"similar_2":simi_2, "similar_3":simi_3,
                       "simiscore_1": simiscore_1,"simiscore_2": simiscore_3, "simiscore_3": simiscore_3}})
