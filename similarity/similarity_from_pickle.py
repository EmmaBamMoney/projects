#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep  3 11:57:05 2019

@author: stan
"""

import pandas as pd
import numpy as np
import math
import pickle
import os
from pymongo import MongoClient
from bson.objectid import ObjectId
import math

client = MongoClient('localhost', 27017)
mydb = client["bam"]
mycol = mydb["assets"]
#df = pd.DataFrame(list(mycol.find()))
#list_id = df['id']
#
#with open('id.pkl', 'wb') as f:
#    pickle.dump(list_id, f)

with open('id_asset_1','rb') as f:
    ids = pickle.load(f)

with open('simi_123_1','rb') as f:
    simi_123 = pickle.load(f)

with open('score_123_1','rb') as f:
    score_123 = pickle.load(f)


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
    id1 = ids[i]
    simiscore_1 = score1[i]
    simiscore_2 = score2[i]
    simiscore_3 = score3[i]
    mycol.update_many({"_id":ObjectId(id1)}, {"$set": {"similar_1": simi_1,"similar_2":simi_2, "similar_3":simi_3,
                       "simiscore_1": simiscore_1,"simiscore_2": simiscore_3, "simiscore_3": simiscore_3}})