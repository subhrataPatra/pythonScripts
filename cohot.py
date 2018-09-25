#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jun  2 02:13:26 2018

@author: subhrata
"""

import pandas as pd
import numpy as np
import os

def startDate(df):
    df["Payment_Date"]= pd.to_datetime(df["Payment Date"])
    df["Payment_YearMonth"]=df["Payment_Date"].map(lambda x: x.strftime('%Y-%m')) #extracting only year and Month
    df.drop(["Payment_Date"], axis=1, inplace=True)
    pivot_df = df.pivot_table(index="Customer Id", values= "Payment_YearMonth",aggfunc=min) #Every users starting YearMonth
    pivot_df.to_csv('dummy.csv')
    pivot_df = pd.read_csv("dummy.csv", index_col= False,  names = ["Customer Id", "Start_Month"])
    df=df.merge(pivot_df,how='left', on="Customer Id")
    df = df.drop_duplicates(["Customer Id", "Payment_YearMonth"]) #if a user has 2 transactions in one months, we are considering it as only one
    return df

def Ranking(df): 
    pivot_df = df.pivot_table(index="Payment_YearMonth")
    pivot_df.to_csv('dummy.csv')
    pivot_df = pd.read_csv("dummy.csv", index_col=0)
    os.remove('dummy.csv')
    pivot_df=pivot_df.reindex(pd.period_range(pivot_df.index[0],pivot_df.index[-1],freq='M')) #to get the months where we do not have any payment
    del pivot_df["Amount"]
    pivot_df.to_csv('dummy1.csv')
    pivot_df1 = pd.read_csv("dummy1.csv", index_col= False , names = ["Payment_YearMonth"] )
    pivot_df1=pivot_df1.drop(pivot_df1.index[[0]])
    pivot_df1['Rank_Payment'] = pivot_df1['Payment_YearMonth'].rank(ascending=1)
    df=df.merge(pivot_df1,how='left', on="Payment_YearMonth") 
    pivot_df2 = pd.read_csv("dummy1.csv", index_col= False , names = ["Start_Month"] )
    pivot_df2=pivot_df2.drop(pivot_df2.index[[0]])
    pivot_df2['Rank_Start'] = pivot_df2['Start_Month'].rank(ascending=1)
    df=df.merge(pivot_df2,how='left', on="Start_Month") 
    os.remove('dummy1.csv')
    return df, pivot_df2

def matrix(df,df_AllMonth):
    months=int(df.Rank_Payment.max())
    Matrix = [['' for y in range(0,months)] for x in range(0,months+1)] #Matrix to print no. of customers in every month and continuing in corresponding months
    Matrix_perc=[['' for y in range(0,months)] for x in range(0,months+1)] #Matrix to print no. of customers in every month and percentage of users in corresponding months
    All_Months=df_AllMonth["Start_Month"]
    Matrix[0][:]=All_Months #all the required months in the first row of matrix
    Matrix_perc[0][:]=All_Months
    df['New'] = np.where(df['Rank_Start'] == df['Rank_Payment'], 1, 0) #to identify if it is a new customers
    df_new= df[df['New']==1] #all new customers
    df_other=df[df['New']==0] #all old customers
    for i in range(1,months+1):
        for j in range(i,months+1):
            if (j==i):
                Matrix[i][j-1]= len(df_new[df_new.Rank_Payment == i]) #number of users starting in a perticular months
                Matrix_perc[i][j-1]= Matrix[i][j-1]
            else:
                if(Matrix[i][i-1])==0:
                    Matrix[i][j-1]=len(df_other[(df_other.Rank_Payment == j )& (df_other.Rank_Start==i)]) #number of users continuing
                    Matrix_perc[i][j-1]= Matrix[i][j-1]
                else:
                    Matrix[i][j-1]=len(df_other[(df_other.Rank_Payment == j )& (df_other.Rank_Start==i)]) #number of users continuine
                    Matrix_perc[i][j-1]=float((float(Matrix[i][j-1])/float(Matrix[i][i-1]))*100) #perc of users continuing
                    Matrix_perc[i][j-1]= format( Matrix_perc[i][j-1], '.2f')
    num_df = pd.DataFrame(Matrix)
    num_df.to_csv('out_num.csv', index=False, header=False)
    perc_df=pd.DataFrame(Matrix_perc)
    perc_df.to_csv('out_perc.csv', index=False, header=False)
    print("done")
    
    
df = pd.read_csv("assignment_data.csv")
df_start=startDate(df)
master_df,df_AllMonth=Ranking(df_start)
matrix(master_df,df_AllMonth)





