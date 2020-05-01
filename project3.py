# -*- coding: utf-8 -*-
"""
Created on Fri May  1 01:22:17 2020

@author: saipavankalyan,sai sandeep reddy
"""

from pymongo import MongoClient 

  


def location(collection):
    #The Location where more accidents are happening
    accident=collection.aggregate([{'$group' : {'_id' : '$LocalAuthority',
                                                'highest_accidents_city' : {'$sum' : 1}}},
                                                {'$sort':{'highest_accidents_city':-1}},{'$limit':1}])
    #Printing the data inserted 
    totalaccident=list(accident)
    print("The Location were more accidents are happening : ",totalaccident[0]["_id"])
    
def severity(collection):
#Severity at Particular Location
    serv = input("enter a city name:")
    severe=collection.aggregate([{'$group':{'_id' : '$LocalAuthority',
                                            'percent': {'$avg':{'$toDouble':'$Accident_Severity'}}}},
                                            {'$match':{'_id':serv}}])
    servloc=list(severe)
    print('Severity at '+serv+" is",servloc[0]["percent"])
    
    #sPEED LIMIT
def speedlimit(collection):
    speed=collection.aggregate([{'$group' : {'_id' : '$Speed_limit', 'Frequency' : {'$sum' : 1}}},{'$sort':{'Frequency':-1}},{'$limit':1}])
    speedlim=list(speed)
    print("At this speed most accidents are happening :",speedlim[0]["_id"]+" mph")
    
def urbanrural(collection):
    Urban=collection.aggregate([{'$group' : {'_id' : '$Urban_or_Rural_Area','Urban':{'$sum':1}}},{'$match':{'_id':'1'}}])
    Rural=collection.aggregate([{'$group' : {'_id' : '$Urban_or_Rural_Area','Rural':{'$sum':1}}},{'$match':{'_id':'2'}}])
    Total=collection.aggregate([{'$group' : {'_id' : 'null',  'Total' : {'$sum' : 1}}},{'$project': {'_id':0}}])
    urbanlist=list(Urban)
    rurallist=list(Rural)
    total=list(Total)
    urbanvalue=urbanlist[0]["Urban"]
    ruralvalue=rurallist[0]["Rural"]
    totalvalue=total[0]["Total"]
    Final=((urbanvalue)/(totalvalue))*100
    Final2=((ruralvalue)/(totalvalue))*100
    print('Urban Percentage =',Final)
    print('Rural Percentage =',Final2)
    
def conditions(collection):
    Light=collection.aggregate([{'$group':{'_id' :{ 'light':'$Light_Conditions','weather':'$Weather_Conditions','Road_Surface':'$Road_Surface_Conditions'},
                                           'Light':{'$sum' :1}}},{'$sort':{'Light':-1}},{'$limit':1}])
    bettercond=list(Light)
    print("The following are the factors contribute to most of accidents :")
    print(bettercond[0]["_id"]['light'])
    print(bettercond[0]["_id"]['weather'])
    print(bettercond[0]["_id"]['Road_Surface'])
    
def day(Week):
    days={
            "1" : "Sunday","2" : "Monday","3" : "Tuesday","4" : "Wednesday","5" : "Thrusday","6" : "Friday","7" : "Saturaday",
            }
    print ("The day of the week which is safest to travel :" ,days.get(Week))
def safeday(collection):
    Weekday=collection.aggregate([{'$group' : {'_id' : '$Day_of_Week', 'Weekday' : {'$sum' : 1}}},{'$sort':{'Weekday':1}},{'$match':{'_id':{'$nin':[None]}}},{'$limit':1}])
    week=list(Weekday)
    day(week[0]["_id"])

def ped_cautious(collection):
    Pedestrains=collection.aggregate([{'$group':{'_id' :{ 'pedestrains':'$Pedestrian_Crossing-Physical_Facilities','junction':'$Junction_Control'},
                                                 'Pedestrains':{'$sum' :1}}},
                                                {'$match':{'_id.pedestrains':{'$nin':[None,'No physical crossing within 50 meters']}}},
                                                {'$sort':{'Pedestrains':-1}},{'$limit':1}])
    ped=list(Pedestrains)
    print("At this signals pedestrains must be cautious:", ped[0]["_id"]["junction"])
    
def monthchar(no_of_month):
    months={
            "01" : "January","02" : "February","03" : "March","04" : "April","05" : "May","06" : "June","07" : "July","08" : "August","09" : "September","10" : "October","11" : "November","12" : "December"
            }
    return months[no_of_month]
def casualites(collection):
    monthstr=collection.aggregate([{'$project':{'month123':{'$substr':['$Date',3,2]}}},
                                                            {'$group':{'_id':'$month123','sum':{'$sum':1}}},
                                                            {'$sort':{'_id':1}},
                                                            {'$match':{'_id':{'$nin':['']}}}])
    high_month=list(monthstr)
    for month in high_month:
        print(monthchar(month["_id"])+":",month["sum"])
    

if __name__=="__main__":
    try: 
        conn = MongoClient() 
        print("Connected successfully!!!") 
    except:   
        print("Could not connect to MongoDB") 
  
    # database 
    db = conn.database 
    collection = db.project3
    flag =1
    while flag:
        print()
        print("**********Welcome to Menu***************")
        print("1. Average Severity at Particular location")
        print("2. No. of Casualties per month")
        print("3. The Location where more accidents are happening ")
        print("4. which day of the week is safe to travel")
        print("5. Percentage of Urban and Rural")
        print("6. At which speed limit most accidents happened")
        print("7. What factors contribute to most of accidents")
        print("8. At which Traffic signals pedestrian must be cautious regarding traffic")
        print("9. exit")
        option=input("Select your Option: ")

        if option=="1":
            severity(collection)
        elif option =="2":
            casualites(collection)
        elif option=="3":
            location(collection)
        elif option=="4":
            safeday(collection)
        elif option=="5":
            urbanrural(collection)
        elif option=="6":
            speedlimit(collection)
        elif option=="7":
            conditions(collection)
        elif option=="8":
            ped_cautious(collection)
        elif option=="9":
            break
        else:
            print("enter a valid input")
            
