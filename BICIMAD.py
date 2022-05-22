from pyspark import SparkContext, SparkConf
import json
from pprint import pprint
from datetime import date
import sys


def initSC(cores, memory):
    conf = SparkConf()\
           .setAppName(f"Bicimad Return Trips {cores} cores {memory} mem")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    return sc

def get_weekday(line):
    data = json.loads(line)
    s_date = data["unplug_hourTime"]
    if isinstance(s_date, str):
        s_date = s_date[0:10]
        data["unplug_hourTime"] = date.fromisoformat(s_date).isoweekday()
    else:
        s_date = s_date['$date'][0:10]
        data["unplug_hourTime"] = date.fromisoformat(s_date).isoweekday()
    return data

def get_ages(rdd):
    trips = rdd.map(get_weekday)\
        .filter(lambda x: x["ageRange"]!=0 )\
        .map(lambda x: (x["idunplug_station"], (x["unplug_hourTime"], x["ageRange"],x["idplug_station"])))\
        .groupByKey().mapValues(list)\
        .map(average_age_and_final_station).sortBy(lambda x: x)\
        .map(forma)
    return trips.collect()


def forma(user_info):
    estacion = user_info[0]
    edad_lun_vier = user_info[1][0]
    edad_sab_dom = user_info[1][1]
    est_lun_vier = user_info[1][2]
    est_sab_dom = user_info[1][3]
    return { 
          "Estación": estacion, 
          "Rango edad" : { "Lunes-Viernes": edad_lun_vier ,"Sabado-Domingo": edad_sab_dom},
          "Suelen ir a": {"Lunes-Viernes": est_lun_vier ,"Sabado-Domingo": est_sab_dom},
          }


def average_age_and_final_station(tupla):
    result_weekend= [0,0,0,0,0,0]
    result_week = [0,0,0,0,0,0]
    result_station_week = [0] * 219
    result_station_weekend = [0] *219
    for terna in tupla[1]:
        if (terna[0] == 6 or terna[0] == 7 ):
            result_weekend[terna[1]-1] = result_weekend[terna[1]-1] +1 
            if terna[2] != tupla[0]:
                result_station_weekend[terna[2]-1] = result_station_weekend[terna[2]-1] +1
        else:
            result_week[terna[1]-1] = result_week[terna[1]-1] +1 
            if terna[2] != tupla[0]:
                result_station_week[terna[2]-1] = result_station_week[terna[2]-1] +1
    maximo_weekend = result_weekend[0]
    maximo_week = result_week[0]
    maximo_station_week = result_station_week[0]
    maximo_station_weekend = result_station_weekend[0]
    for elem in result_weekend:
        if elem > maximo_weekend:
            maximo_weekend = elem
    for elem in result_week:
        if elem > maximo_week:
            maximo_week = elem
    for elem in result_station_week:
        if elem > maximo_station_week:
            maximo_station_week = elem
    for elem in result_station_weekend:
        if elem > maximo_station_weekend:
            maximo_station_weekend = elem
    return (tupla[0],(result_week.index(maximo_week) + 1, result_weekend.index(maximo_weekend) + 1, 
            result_station_week.index(maximo_station_week) + 1,
            result_station_weekend.index(maximo_station_weekend) + 1))
      


def main(sc, years, months):
     rdd = sc.parallelize([])
     for y in years:
         y = int(y)
         for m in months:
             m = int(m)
             if m < 10:
                 filename = f"{y}0{m}_Usage_Bicimad.json"
             else:
                 filename = f"{y}{m}_Usage_Bicimad.json"
             print(f"Adding {filename}")
             rdd = rdd.union(sc.textFile(filename))
     trips = get_ages(rdd)
     print("............Starting computations.............")
     pprint(trips)
    

if __name__=="__main__":
    if len(sys.argv)<2:
        years = [2018]
    else:
        years = list(map(int, sys.argv[1].split()))

    if len(sys.argv)<3:
        months = [3]
    else:
        months = list(map(int, sys.argv[2].split()))

    print(f"years: {years}")
    print(f"months: {months}")

    with initSC(0,0) as sc:
        main(sc, years, months)
