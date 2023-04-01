#Morphdata=new fitbit process()

import pandas as pd
import glob as gb
import numpy as np
import os.path
import sys
pd.set_option('display.max_rows', 20000)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
#pd.set_option('display.max_rows', 6000)

#Importing the library of device data
class ProcessFitbit:
    def __init__ (self,location='Data/User/00**'):
        #orient program to proper location of fitbit files, sort files ascending order
        Userlist= gb.glob(location)
        Userlist.sort()

    def build(self,type='both'):
        #if rebuildIDs==False:
        #create startdate plus IDs function
           #self.startdate()
        if type == 'steps' or type == 'both':
            self.process_steps()
            self.steps_summarized.to_excel('Output/ImportSteps40+.xlsx')
    

        if type == 'activity' or type == 'both':
            self.process_activity_minutes()
            self.activity_minutes_summarized.to_excel('Output/ImportActivityMinutes.xlsx')

        if type == 'both':
            self.steps_minutes_summarized = self.steps_summarized.merge(self.activity_minutes_summarized, on=['ID','Date'])
            self.steps_minutes_summarized.to_excel('Output/ImportStepsActivityMinutes.xlsx')

    def process_steps(self):
        #To get files containing daily steps, and sort files in ascending order,amass minute-level steps into a dataframe with PID and Timestamped both dateTime and Date and Time
        # open source list and then close it, slice the source file into iterations that can be placed into columns, add participants step file together, add participants together into
        step_file_list = gb.glob('Data/User/**/steps-*', recursive= True)
        step_file_list.sort()
        self.steps_summarized=[]
        for file in step_file_list:
            filestream = open(file)
            steps=pd.read_json(filestream)
            filestream.close()
            Id,Id2,Folder, Typeoffolder, stepfile = file.split("/",4)
            steps.rename(columns= {'value':'steps'}, inplace = True)
            steps['Date'] = pd.to_datetime(steps['dateTime']).dt.date
            steps['Time'] = pd.to_datetime(steps['dateTime']).dt.time
            steps['ID'] = Folder
            steps=steps[['ID','Date','Time','dateTime','steps']]
            self.steps_summarized.append(steps)
        self.steps_summarized=pd.concat(self.steps_summarized, axis=0, ignore_index=True)
        self.steps_summarized['ID'] = self.steps_summarized['ID'].astype('int')
        self.steps_summarized['Time'] = pd.to_datetime(self.steps_summarized['Time'], format='%H:%M:%S')
        self.steps_summarized = (self.steps_summarized[self.steps_summarized.ID > 39])
        #print(self.steps_summarized)

        #read in week (need to have option if it doesn't exist)\
    def startdate(self):
        self.startdateids = pd.read_excel("Ids/MorphIDs.xlsx")
        self.startdateids.rename(columns={'V1 Visit Dates': 'startdate'}, inplace=True)
        self.startdateids.rename(columns={'PID': 'ID'}, inplace=True)
        self.startdateids['ID'] = self.startdateids['ID'].astype('int')
        self.startdateids.set_index('ID', inplace=True)

    def process_activity_minutes(self, type_array=['sedentary_minutes','lightly_active_minutes','moderately_active_minutes','very_active_minutes']):
        self.type_total = pd.DataFrame()


        for type in type_array:
            #grab files related to type of activity minutes
            minute_list = gb.glob('Data/User/**/'+type+'*', recursive=True)
            #sort files ascending
            minute_list.sort()

            for file in minute_list:
                #read in file contents of type of activity minutes
                filestream = open(file)
                minutes = pd.read_json(filestream)
                filestream.close()
                #split file into useable keys
                Data, User, ID, Id2, Typeoffolder, minutefile = file.split("/", 5)
                # Labeling the df with proper headings and adding separated date and time:
                minutes['type'] = type
                minutes['Date'] = pd.to_datetime(minutes['dateTime']).dt.date
                minutes['Time'] = pd.to_datetime(minutes['dateTime']).dt.time
                minutes['ID'] = ID
                minutes['ID'] = minutes['ID'].astype('int')
                minutes = minutes[['ID', 'Date', 'type', 'value']]
                self.type_total=pd.concat([self.type_total,minutes], ignore_index=True)
        self.activity_minutes_summarized=self.type_total.groupby(['ID','Date','type'])['value'].agg('first').unstack().reset_index()

    def readsummarized(self, type='both'):
        #Relabel ** with the folder name you'd like to read in

        if type == 'both':
            path = '/Users/justinrobison/PycharmProjects/pythonProject1/Output/ImportStepsActivityMinutes.xlsx'
            isExist= os.path.exists(path)
            print('processed steps and activity minutes T/F')
            print(isExist)
            #check if the ImportStepsActivityMinutes File exists, if not do error`
            self.processed_df = pd.read_excel('Output/ImportStepsActivityMinutes.xlsx')
        if type == 'steps':
            path1 = '/Users/justinrobison/PycharmProjects/pythonProject1/Output/ImportSteps0-39.xlsx'
            path2 = '/Users/justinrobison/PycharmProjects/pythonProject1/Output/ImportSteps40+.xlsx'
            isExist1 = os.path.exists(path1)
            isExist2 = os.path.exists(path2)
            print('processed steps 0-39 T/F')
            print(isExist1)
            print('processed steps 40+ T/F')
            print(isExist2)

            self.processed_df039 = pd.read_excel('Output/ImportSteps0-39.xlsx',index_col=0)
            self.processed_df40up = pd.read_excel('Output/ImportSteps40+.xlsx',index_col=0)
            self.processed_df=pd.concat([self.processed_df039,self.processed_df40up])
            #print(self.processed_df)
            self.processed_df_nonzero = self.processed_df[self.processed_df['steps'] != 0]
            #print(self.processed_df_nonzero)

            #print(self.processed_df)
            #self.processed_df2 = self.processed_df.groupby(['ID', 'Date']).agg({'steps': 'sum'}).reset_index()
            #print(self.processed_df2)
            #self.processed_df2.to_excel('Output/processed_df_steps.xlsx')

        if type == 'activity':
            path = '/Users/justinrobison/PycharmProjects/pythonProject1/Output/ImportStepsActivityMinutes.xlsx'
            isExist = os.path.exists(path)
            #print('processed activity minutes T/F')
            #print(isExist)
            self.processed_df_activity = pd.read_excel('Output/ImportActivityMinutes.xlsx')
            self.processed_df_activity = self.processed_df_activity[self.processed_df_activity.sedentary_minutes != 1440]
            self.processed_df_activity = self.processed_df_activity[(self.processed_df_activity.select_dtypes(include=['number']) != 0).any(1)]
            self.processed_df_activity2 = self.processed_df_activity.eval('total_active_minutes = lightly_active_minutes + moderately_active_minutes + very_active_minutes')
            self.processed_df_activity2 = self.processed_df_activity2[
                ['ID', 'Date', 'sedentary_minutes', 'total_active_minutes', 'lightly_active_minutes',
                 'moderately_active_minutes', 'very_active_minutes']]
            #print(self.processed_df_activity2)

    def aggregate(self, type='day'):
        self.processed_df = self.processed_df.groupby(['ID', 'Date']).agg({'steps': 'sum'}).reset_index()
        #self.importeddf?
        if type == 'week_intervention':
            self.startdate()
            self.processed_df=pd.merge(self.processed_df,self.startdateids, on="ID")
            self.processed_df['intervention_week'] = np.floor(((self.processed_df.Date - self.processed_df.startdate) / np.timedelta64(1, 'W'))) + 1
            self.processed_df = self.processed_df[['ID','Date','intervention_week','steps','sedentary_minutes','lightly_active_minutes','moderately_active_minutes','very_active_minutes']]
            self.processed_df_sum = self.processed_df.groupby(['ID', 'intervention_week']).agg({'steps': 'sum'}).reset_index()
            self.processed_df_std = self.processed_df.groupby(['ID', 'intervention_week']).agg({'steps': 'std'}).reset_index()
            self.processed_df_std.rename(columns={'steps': 'std'}, inplace=True)
            self.processed_df_week_intervention = pd.merge(self.processed_df_sum, self.processed_df_std,on=['ID', 'intervention_week'])
            print(self.processed_df_week_intervention)

        if type == 'week_calendar':
            #try to pull off year first
            self.processed_df['week_number'] = self.processed_df['Date'].dt.isocalendar().week
            #print(self.processed_df)
            self.processed_df = self.processed_df[['ID','Date','week_number','steps','sedentary_minutes','lightly_active_minutes','moderately_active_minutes','very_active_minutes']]
            self.processed_df_sum = self.processed_df.groupby(['ID','week_number']).agg({'steps':'sum'}).reset_index()
            self.processed_df_std = self.processed_df.groupby(['ID','week_number']).agg({'steps':'std'}).reset_index()
            self.processed_df_std.rename(columns={'steps':'std'}, inplace=True)
            self.processed_df_week_calendar = pd.merge(self.processed_df_sum,self.processed_df_std, on=['ID','week_number'])
            print(self.processed_df_week_calendar)

        if type == 'month':
            #needs work
            self.month_df = self.importeddf.groupby(['ID', 'intervention_week']).agg({'TotalDailySteps': 'sum'}).reset_index()
            #chunk of code used to combine ids with steps, and put in 'intervention week' column
            #self.importdf = pd.merge(self.stepsall, self.startdateids, on="ID")
            #self.importdf['Date'] = pd.to_datetime(self.importdf['Date'])
            #self.importdf['intervention_week'] = np.floor(((self.importdf.Date - self.importdf.startdate) / np.timedelta64(1, 'W'))) + 1

    def applyfilter(self, type ='null'):
        if type=='time':
            #get rid of values in DF that are zero
            self.minutesclean = self.minutesclean[(self.minutesclean.select_dtypes(include=['number']) != 0).any(1)]
            # get rid of columns that contain 1440 sedentary minutes (means not actually wearing device)
            self.minutesclean = self.minutesclean[self.minutesclean.sedentary_minutes != 1440]
        #if type=='week':
            #for [i=0,i=24, i++]
            #self.weekarray.push(i)
            #self.weekfilterdf = (self.timefilterdf.loc[self.timefilterdf['intervention_week'].isin(self.weekarray)])
        if type=='daysince':
            self.weekfilterdf['startdate'] = pd.to_datetime(self.weekfilterdf['startdate']).dt.date
            self.weekfilterdf['Daysfromstart'] = (self.weekfilterdf['Date'] - self.weekfilterdf['startdate']).dt.days
        if type=='minutesactive':
            #only take active mintues totals greater than 1 (means that they were active)
            self.timefilterdf = (self.giantdf[self.giantdf.total_active_minutes > 1])

    def steppatterning(self):
        self.thishour=None
        self.lasthour=None
        self.lastid = None
        self.lastdate = None
        self.lasttime = None
        self.thisid = None
        self.thisdate = None
        self.thistime = None
        self.dailystore = []
        self.boutlength = 0
        self.Firsttime = True
        self.summarydf = pd.DataFrame(columns=['ID', 'Date', 'breaks', 'medianboutlength', 'avgboutlength','array','5min','10min'])
        for row in self.processed_df_nonzero.itertuples():
            if self.Firsttime == True:
                self.Firsttime = False
                self.lastid = row[1]
                self.lasttime = row[3]
                self.lastdate = row[2]
                #print(self.lasthour)
            self.thisid = getattr(row, 'ID')
            self.thisdate = getattr(row, 'Date')
            if self.thisdate != self.lastdate or self.thisid != self.lastid:
                if 0 in self.dailystore:
                    self.dailystore.remove(0)
                self.dailystore.append(self.boutlength)
                # sum, med, avg, and count of dailystorage
                self.sumdf = sum(self.dailystore)
                self.lendf = len(self.dailystore)
                self.mediandf = np.median(self.dailystore)
                self.avgdf = self.sumdf / self.lendf
                self.stddf = np.std(self.dailystore)
                # create temporary df to hold the day's values
                tempdf = pd.DataFrame(columns=['ID', 'Date', 'breaks', 'medianboutlength', 'avgboutlength', 'stdboutlength','array','5min','10min'],index=[0])
                tempdf['ID'] = self.thisid
                tempdf['Date'] = self.lastdate
                tempdf['breaks'] = self.lendf
                tempdf['medianboutlength'] = self.mediandf
                tempdf['avgboutlength'] = self.avgdf
                tempdf['stdboutlength'] = self.stddf
                string= ','.join(str(x) for x in self.dailystore)
                tempdf['array'] = string
                # push temporary df to high level df
                dailystore5min = [i for i in self.dailystore if i >= 5]
                tempdf['5min'] = len(dailystore5min)
                # print(tempdf)
                dailystore10min = [i for i in self.dailystore if i >= 10]
                tempdf['10min'] = len(dailystore10min)
                self.summarydf = pd.concat([self.summarydf, tempdf], ignore_index=True)
                # reset values and empty counters
                self.lasttime = self.thistime
                self.lastdate = self.thisdate
                self.lastid = self.thisid
                self.boutlength = 0
                self.dailystore = []
            self.futuremin = None
            self.futuremin = self.lasttime + pd.Timedelta(minutes=2)
            self.thistime = getattr(row, 'Time')
            if self.thistime <= self.futuremin:
                self.boutlength = self.boutlength + 1
                self.lasttime = self.thistime
            else:
                self.dailystore.append(self.boutlength)
                self.boutlength = 1
                self.lasttime = self.thistime
        print(self.summarydf)
        self.summarydf.to_excel('Output/patterning2min3.14.xlsx')

    def hourpatterning(self):
        self.lastid = None
        self.lastdate = None
        self.lasttime = None
        self.thisid = None
        self.thisdate = None
        self.thistime = None
        self.hourstore = []
        self.hourbout = 0
        self.Firsttime = True
        self.lasthour = None
        self.summaryhourdf = pd.DataFrame(columns=['ID', 'Date', 'Hour', 'count','summedminutes'])
        # futuremin=lasttime+1
        for row in self.processed_df_nonzero.itertuples():
            if self.Firsttime == True:
                self.Firsttime = False
                self.lastid = row[1]
                self.lasttime = row[3]
                self.lastdate = row[2]
                self.lasthour = row[4].hour
            self.thisid = row[1]
            self.thisdate = row[2]
            self.thishour = row[4].hour
            self.futuremin = None
            self.futuremin = self.lasttime + pd.Timedelta(minutes=1)
            self.thistime = row[3]
            if self.thisdate == self.lastdate and self.thisid == self.lastid:
                if self.thishour == self.lasthour:
                    if self.thistime == self.futuremin:
                        self.hourbout = self.hourbout + 1
                        self.lasttime = self.thistime
                    if self.thistime != self.futuremin:
                        self.hourstore.append(self.hourbout)
                        self.hourbout = 1
                        self.lasttime = self.thistime
                if self.thishour != self.lasthour:
                    self.hourstore.append(self.hourbout)
                    if 0 in self.hourstore:
                        self.hourstore.remove(0)
                    # print(hourstore)
                    self.hourcount = len(self.hourstore)
                    self.temphourdf = pd.DataFrame(columns=['ID', 'Date', 'Hour', 'count','summedminutes'], index=[0])
                    self.temphourdf['ID'] = self.lastid
                    self.temphourdf['Date'] = self.lastdate
                    self.temphourdf['Hour'] = self.lasthour
                    self.temphourdf['count'] = self.hourcount
                    self.temphourdf['summedminutes'] = sum(self.hourstore)
                    # print(temphourdf)
                    self.summaryhourdf = pd.concat([self.summaryhourdf, self.temphourdf], ignore_index=True)
                    # print(summaryhourdf)
                    self.hourbout = 1
                    self.hourstore = []
                    self.hourcount = None
                    # print(summaryhourdf)
                    self.lastid = self.thisid
                    self.lasthour = self.thishour
                    self.lastdate = self.thisdate
                    self.lasttime = self.thistime
            if self.thisdate != self.lastdate or self.thisid != self.lastid:
                self.hourstore.append(self.hourbout)
                if 0 in self.hourstore:
                    self.hourstore.remove(0)
                # print(hourstore)
                self.hourcount = len(self.hourstore)
                self.temphourdf = pd.DataFrame(columns=['ID', 'Date', 'Hour', 'count','summedminutes'], index=[0])
                self.temphourdf['ID'] = self.lastid
                self.temphourdf['Date'] = self.lastdate
                self.temphourdf['Hour'] = self.lasthour
                self.temphourdf['count'] = self.hourcount
                self.temphourdf['summedminutes'] = sum(self.hourstore)
                # print(temphourdf)
                self.summaryhourdf = pd.concat([self.summaryhourdf, self.temphourdf], ignore_index=True)
                # print(summaryhourdf)
                self.hourbout = 1
                self.hourstore = []
                self.hourcount = None
                # print(summaryhourdf)
                self.lastid = self.thisid
                self.lasthour = self.thishour
                self.lastdate = self.thisdate
                self.lasttime = self.thistime
        self.summeddatehourcountdfavg = self.summaryhourdf.groupby(['ID', 'Date']).agg({'count': 'mean'}).reset_index()
        # print(summeddatehourcountdfavg)
        self.summeddatehourcountdfvar = self.summaryhourdf.groupby(['ID', 'Date']).agg({'count': 'var'}).reset_index().rename(
            columns={'count': 'variance'})
        # print(summeddatehourcountdfvar)
        self.summedtotaldf = self.summeddatehourcountdfavg.merge(self.summeddatehourcountdfvar)
        #print(self.summedtotaldf)
        self.summaryhourdf.to_excel('Output/hourpatterning3.14.xlsx')
        self.summedtotaldf.to_excel('Output/hourpatterning3.14.2.xlsx')


    def combothesis(self):
        self.patterning_df=pd.read_excel('Output/patterning2min3.14.xlsx', index_col=0)
        self.stepsfinal_df = pd.read_excel('Output/processed_df_steps.xlsx', index_col=0)
        print(self.patterning_df)
        print(self.stepsfinal_df)
        self.patterning_df1=self.patterning_df.reset_index().rename(columns={'Date':'DateHour','ID':'ID2'})
        #print(self.patterning_df1)
        #print(self.stepsfinal_df1)
        #self.stepsfinal_df.reset_index()
        #self.thesisdf=pd.concat([self.processed_df3, self.summarydf1],axis=1)
        self.thesisfinal = pd.concat([self.patterning_df1,self.stepsfinal_df], join='inner',axis=1).reset_index()
        #print(self.thesisfinal)
        #print(self.thesisfinal.head())
        #sys.exit()
        #self.thesis_final1=self.thesisfinal.drop(labels='DateHour',axis=1)
        #self.thesis_final1 = self.thesisfinal.drop(labels='ID2', axis=1)
        self.thesis_final1=self.thesisfinal[['ID2','DateHour','steps','breaks','medianboutlength','avgboutlength','stdboutlength','array','5min','10min']]
        print(self.thesis_final1)

        self.hourpatterning_df = pd.read_excel('Output/hourpatterning3.14.2.xlsx', index_col=0)
        print(self.hourpatterning_df)
        self.thesisfinalfinal=pd.concat([self.thesis_final1,self.hourpatterning_df],axis=1)
        print(self.thesisfinalfinal)
        self.thesisfinalfinal.to_excel('Output/THESIS_Data2min.2.xlsx')
        #so this next dataframe drops about 60 rows for whatever reason
        #self.thesisfinal2 = pd.merge(self.thesis_final1,self.processed_df_activity2, how = 'outer')
        #print(self.thesisfinal2)
        #print(self.thesisfinal2.head())


        #READ IN ACTIVITY MINUTES
        #CONCAT ALONG THE THESISFINAL DATAFRAME
        #Arrange properly
        # then figure out variance final measure 
        #self.thesisdf2=self.processed_df_activity2.merge(self.thesisdf
        #self.thesisdf2 = self.processed_df_activity2.merge(self.thesisdf, how='right')

        #print(self.thesisdf2)