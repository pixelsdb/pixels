import os
import sys
import copy
import duckdb
import datetime
import pandas as pd
from statsforecast import StatsForecast
from statsforecast.models import AutoARIMA

#  the split function
#1.split the query to small/mid/big query by the cputime
#2.split the query to different timestrap every 5 mins
def dataprocess(df, startime, delta, startime2, delta2, cpuspl, memspl):
    # G = 1024**3
    # memspl = [G, 8*G, 16*G, 32*G, 64*G, 128*G] #1G 8G 16G 32G 64G 128G
    mems = [{startime2:[]} for i in range(len(memspl)+1)]

    # cpuspl = [10*1000, 60*1000, 5*60*1000, 10*60*1000] #10s 1min 5min 10min
    cpuTimes = [{startime:[]} for i in range(len(cpuspl)+1)]

    for i in df.itertuples():
        if (startime+delta) <= i.createdTime:
            startime += delta
            while (startime+delta) < i.createdTime:
                for j in range(len(cpuspl)+1):
                    cpuTimes[j][startime] = []
                startime += delta
            flag = 0
            for j in range(len(cpuspl)):
                cpuTimes[j][startime] = []
                if (not flag and i.cpuTimeTotal <= cpuspl[j]):
                    cpuTimes[j][startime].append(i.cpuTimeTotal)
                    flag = 1
            cpuTimes[len(cpuspl)][startime] = []
            if not flag:
                cpuTimes[len(cpuspl)][startime].append(i.cpuTimeTotal)
        else:
            flag = 0
            for j in range(len(cpuspl)):
                if i.cpuTimeTotal <= cpuspl[j]:
                    cpuTimes[j][startime].append(i.cpuTimeTotal)
                    flag = 1
                    break
            if not flag:
                cpuTimes[len(cpuspl)][startime].append(i.cpuTimeTotal)
        #----------------------------------------------------------#
        if (startime2+delta2) <= i.createdTime:
            startime2 += delta2
            while (startime2+delta2) < i.createdTime:
                for j in range(len(memspl)+1):
                    mems[j][startime2] = []
                startime2 += delta2
            flag = 0
            for j in range(len(memspl)):
                mems[j][startime2] = []
                if (not flag and i.memoryUsed <= memspl[j]):
                    mems[j][startime2].append(i.memoryUsed)
                    flag = 1
            mems[len(memspl)][startime2] = []
            if not flag:
                mems[len(memspl)][startime2].append(i.memoryUsed)
        else:
            flag = 0
            for j in range(len(memspl)):
                if i.memoryUsed <= memspl[j]:
                    mems[j][startime2].append(i.memoryUsed)
                    flag = 1
                    break
            if not flag:
                mems[len(memspl)][startime2].append(i.memoryUsed)

    return cpuTimes,mems

# use autoARIMA forecast the cputime usage next 5min
def cpuTimeForecast(historyDatas, curtime):
    scale = 5*60*1000 #5min
    cpuTimes = []
    for historyData in historyDatas:
        ds = []
        y = []
        for i in historyData:
            ds.append(i)
            y.append(sum(historyData[i])/scale)
        train_data = pd.DataFrame({'unique_id': [1]*len(historyData), 'ds': ds, 'y': y})
        sf = StatsForecast(models=[AutoARIMA()], freq='5min')
        sf.fit(train_data)
        H = int((curtime-ds[-1])/(datetime.timedelta(minutes=15))) + 2
        cpuTime = (sf.predict(h=H, level=[90]))['AutoARIMA-hi-90'].iloc[-1]
        cpuTimes.append(cpuTime)
    return cpuTimes

def memUasgeForecast(historyDatas, curtime):
    scale = 1024**3 #1G
    memUsages = []
    for historyData in historyDatas:
        ds = []
        y = []
        for i in historyData:
            ds.append(i)
            y.append(sum(historyData[i])/scale)
        train_data = pd.DataFrame({'unique_id': [1]*len(historyData), 'ds': ds, 'y': y})
        sf = StatsForecast(models=[AutoARIMA()], freq='5min')
        sf.fit(train_data)
        H = int((curtime-ds[-1])/(datetime.timedelta(minutes=15))) + 2
        memUsage = (sf.predict(h=H, level=[90]))['AutoARIMA-hi-90'].iloc[-1]
        # memUsage = [i/300 for i in memUsage]
        memUsages.append(memUsage)
    return memUsages



def main():
    logfile = '\'' + sys.argv[1] + '\''
    if os.path.exists(sys.argv[2]):
        logfile = logfile + ','  + '\'' + sys.argv[2] + '\''
    cpuspl = [int(i) for i in sys.argv[3][1:-1].split(',')]
    memspl = [int(i)*(1024**3) for i in sys.argv[4][1:-1].split(',')]
    curtime = datetime.datetime.now().replace(microsecond=0)
    startime = curtime - datetime.timedelta(days=7)
    que = " select createdTime, cpuTimeTotal, memoryUsed from read_csv_auto([" \
          + logfile \
          + "]) where createdTime between '" \
          + str(startime) \
          + "' and '" \
          + str(curtime) \
          + "' order by createdTime"
    df = duckdb.query(que).df()

    delta = datetime.timedelta(minutes=5)
    cpuTimeData,memUsageData = dataprocess(df, startime, delta, startime, delta, cpuspl, memspl)
    
    cpuTimes = cpuTimeForecast(cpuTimeData, curtime)
    print(*cpuTimes)
    memUsages = memUasgeForecast(memUsageData, curtime)
    print(*memUsages)

main()
