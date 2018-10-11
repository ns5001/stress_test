import pandas as pd
import numpy as np
import requests
import multiprocessing as mp

#path='X:/Transportation/Yijun/STRESSTEST/' # already put the test file there
path='/Users/NaveedShah/Desktop/'
key='testkey' # input for the key
numthreads=10 # change to the number of threads to use; typically maximum is cpu number-1

address=pd.read_csv(path+'DMV.csv',dtype=str)
address['LAT']=np.nan
address['LONG']=np.nan


def geoserivce(df):
    for i in df.index:
        url='http://geosupport-loadbalancer-01-1284729165.us-east-1.elb.amazonaws.com/GeoService/GeoService.svc/Function_1B?key='+key
        url+='&StreetName='+df.loc[i,'STREET ADDRESS']+' '+df.loc[i,'CITY & STATE']+' '+df.loc[i,'ZIP']
        url+='&Borough='+df.loc[i,'BoroCode']
        req=requests.get(url)
        js=req.json()
        if js['display']['out_grc'] in ['00','01']:
            df.loc[i,'LAT']=js['display']['out_latitude']
            df.loc[i,'LONG']=js['display']['out_longitude']
        else:
            print(df.loc[i,'DMVID']+' is not geocoded')
    return df    
    
def parallelize(data, func):
    data_split = np.array_split(data,numthreads)
    pool = mp.Pool(numthreads)
    dt = pd.concat(pool.map(func, data_split))
    pool.close()
    pool.join()
    return dt


if __name__=='__main__':
    address=parallelize(address,geoserivce)
    address.to_csv(path+'output.csv',index=False)