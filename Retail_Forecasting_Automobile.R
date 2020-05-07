################ Retail Forecasting ##################
####### Development Date : 31 May 2019 ########
##### Author : Commercial Automobile Analytics #######

## Objective : To forecast Retail sales on a monthly level across Pan india,
## Region, State & Dealer at RNP level 

## Clear screen ##
rm(list=ls())
gc()
options(max.print = 99999)
options(scipen = 99999)
a <- Sys.time() ## Set start time

## Import Libraries ##
library(xlsx)
library(lubridate)
library(forecast)
library(tseries)
library(dplyr)
library(splitstackshape)
library(smooth)
library(sqldf)
library(data.table)
set.seed(1234) ## Set seed ##

################################################################################################
#Import Retail Data & other files#
################################################################################################

setwd("xxxx") # Choose the directory #
df= read.csv("Opty_Invoice_02012020.csv", header =  TRUE, stringsAsFactors = F) # Read the Opty Inv File #
Assumptions<- read.csv("MHCV_Start_Date_Merged_New_RNP_23122019.csv",stringsAsFactors = F) # Read the file for IAL Norms
RNP_Mapping <- read.csv("MHCV_RNP1_Tonnage_Mapping_23122019.csv") #Master RNP Mapping 
direct_bill <- read.csv("Direct_Billing_02012020.csv", header =TRUE, stringsAsFactors=F) # Read the File for Direct Billing#

################################################################################################
#Cleaning the Opty Inv File#
################################################################################################

#Removing special characters from column's names
col_List <- colnames(df) # Define the list with Column names 
col_List <- tolower(gsub("[^A-Za-z0-9]+", "", col_List)) # remove spaces and special characters 
colnames(df) <- col_List # Place the column name back with the removed spaces 

#Removing Special Character from vc, rnp1,lob,state, region, delaercode column values # 
list<-c("vc","lob","pl","ppl","state","region","dealercode","dealer") # Define a list of column whose vlaues are to be cleaned 
df[,list]<- sapply(df[,list], function(x)  toupper(gsub( pattern = "[^A-Za-z0-9]+", x=x,""))) # clean the values

##Subsetting data for MHCV
df <- subset(df, df$lob %in% c("HCVCARGO","MCVCARGO","MHCVCONST"))

##Subsetting data for Required Regions
unique(df$region)
df <- subset(df, df$region %in% c("WEST","SOUTH","EAST","NORTH"))

#Replacing NA with 0 in Retails
df[is.na(df$retail), "retail"] <- 0
df <- subset(df, df$retail > 0)

#Convert Date format to Date
df$yearmonth <- as.Date(df$yearmonth, "%m/%d/%Y")
sort(unique(df$yearmonth)) # sort the date to check the min and max date 
df$startdate <- as.Date(df$startdate, "%m/%d/%Y")

################################################################################################
# Cleaning the Assumptions (IAL NORMS)  File#
################################################################################################

Assumptions$New_Start_date <- as.Date(Assumptions$New_Start_date, "%m/%d/%Y")
list<-c("RNP1","Merged_RNP") # Define a list of column whose vlaues are to be cleaned 
Assumptions[,list]<- sapply(Assumptions[,list], function(x)  toupper(gsub( pattern = "[^A-Za-z0-9]+", x=x,""))) # clean the values
Assumptions$New_Start_date <- as.Date(Assumptions$New_Start_date, "%m/%d/%Y")

################################################################################################
#Cleaning the DIRECT BILLING FILE #
################################################################################################

#Removing special characters from column's names
col_List <- colnames(direct_bill) # Define the list with Column names 
col_List <- tolower(gsub("[^A-Za-z0-9]+", "", col_List)) # remove spaces and special characters 
colnames(direct_bill) <- col_List # Place the column name back with the removed spaces 

#Removing Special Character from vc, rnp1,lob,state, region, delaercode column values # 
list<-c("vc","lob","pl","ppl","state","region","dealercode","dealer") # Define a list of column whose vlaues are to be cleaned 
direct_bill[,list]<- sapply(direct_bill[,list], function(x)  toupper(gsub( pattern = "[^A-Za-z0-9]+", x=x,""))) # clean the values

##Subsetting data for MHCV
direct_bill <- subset(direct_bill, direct_bill$lob %in% c("HCVCARGO","MCVCARGO","MHCVCONST"))

##Subsetting data for Required Regions
unique(direct_bill$region)
direct_bill <- subset(direct_bill, direct_bill$region %in% c("WEST","SOUTH","EAST","NORTH"))

##Removing cases where Retail Direct Billing is zero
#Replacing NA with 0 in Retails
direct_bill[is.na(direct_bill$retaildirectbilling), "retaildirectbilling"] <- 0
direct_bill <- subset(direct_bill, direct_bill$retaildirectbilling > 0)

#Convert Date format to Date
direct_bill$yearmonth <- as.Date(direct_bill$yearmonth,"%m/%d/%Y")
sort(unique(direct_bill$yearmonth)) # sort the date to check the min and max date 
direct_bill$startdate <- as.Date(direct_bill$startdate, "%m/%d/%Y")

################################################################################################
# Cleaning the RNP Mapping #
################################################################################################

##Removing rows that do not have RNP Mapping###
No_RNP_mapping<-c(" ","#N/A","","#VALUE!")
RNP_Mapping<-RNP_Mapping[!(RNP_Mapping$RNP1 %in% No_RNP_mapping),]

#Removing special characters from column's names
col_List <- colnames(RNP_Mapping) # Define the list with Column names 
col_List <- tolower(gsub("[^A-Za-z0-9]+", "", col_List)) # remove spaces and special characters 
colnames(RNP_Mapping) <- col_List # Place the column name back with the removed spaces 

#Removing Special Character from vc, rnp1,tonnage column values # 
list<-c("vc","rnp1","tonnage","rnp2","modelcategory","tonnageaggregate") # Define a list of column whose vlaues are to be cleaned 
RNP_Mapping[,list]<- sapply(RNP_Mapping[,list], function(x)  toupper(gsub( pattern = "[^A-Za-z0-9]+", x=x,""))) # clean the values

#List of MTO VCs
RNP_Mapping_MTO <- unique(subset(RNP_Mapping[,c("rnp1","modelcategory")], RNP_Mapping$modelcategory=="MTO"))

################################################################################################
# RNP Mapping to OPTY INV & DIRECT BILLING FILE#
################################################################################################

##Mapping RNP1,RNP2 and Tonnage in the opty inv data from RNP Tonnage Sheet
df<-merge(df,RNP_Mapping[,c("vc","rnp1","rnp2","tonnage","tonnageaggregate")], 
          by="vc",  all.x=T)

###Dropping RNP's that are MTO
#df_all <- df
#df<- subset(df, !(df$rnp1 %in% RNP_Mapping_MTO$rnp1))
#print(paste0("Drop in retail of ",sum(df_all$retail)-sum(df$retail),"(",round(1-(sum(df$retail)/sum(df_all$retail)),4),"%)",
#            " from Opty Invoice due to ",length(unique(df_all$vc))-length(unique(df$vc))," MTO vc"))

#Dropping the rows for which we do not have mappings
df_org <-df
df <-subset(df, !(is.na(df$rnp1)))
print(paste0("Drop in retail of ",sum(df_org$retail)-sum(df$retail),"(",round(1-(sum(df$retail)/sum(df_org$retail)),4),"%)",
             " from Opty Invoice due to ",length(unique(df_org$vc))-length(unique(df$vc))," missing vc"))

##Mapping RNP1,RNP2 and Tonnage in the direct billing data from RNP Tonnage Sheet
direct_bill<-merge(direct_bill,RNP_Mapping[,c("vc","rnp1","rnp2","tonnage","tonnageaggregate")], 
                   by="vc",  all.x=T)


##Dropping RNP's that are MTO
#direct_bill_all <- direct_bill
#direct_bill<- subset(direct_bill, !(direct_bill$rnp1 %in% RNP_Mapping_MTO$rnp1))
#print(paste0("Drop in retail of ",sum(direct_bill_all$retail)-sum(direct_bill$retail),"(",round(1-(sum(direct_bill$retail)/sum(direct_bill_all$retail)),4),"%)",
#             " from Opty Invoice due to ",length(unique(direct_bill_all$vc))-length(unique(direct_bill$vc))," MTO vc"))

#Dropping the rows for which we do not have mappings
direct_bill_1 <- direct_bill
direct_bill <-subset(direct_bill, !(is.na(direct_bill$rnp1)))
print(paste0("Drop in Retial of ",sum(direct_bill_1$retaildirectbilling)-sum(direct_bill$retaildirectbilling),"(",round(1-(sum(direct_bill$retaildirectbilling)/sum(direct_bill_1$retaildirectbilling)),4),"%)",
             " from Direct Billing due to ",length(unique(direct_bill_1$vc))-length(unique(direct_bill$vc))," missing VC"))


################################################################################################
# Creation of consolidated Retial with OPTY INV & DIRECT BILLING FILE#
################################################################################################

#Creating a mapping key
direct_bill$key_db <- paste(direct_bill$vc, direct_bill$dealercode, direct_bill$yearmonth, sep="-")
df$key_db <- paste(df$vc, df$dealercode, df$yearmonth, sep = "-")

#Mapping the retail direct billing to the opty data
df <- merge(df, direct_bill[,c("retaildirectbilling","key_db") ], by="key_db",all.x=T)

#Mapping in the rows that are present additionally in the direct billing piece
direct_bill_NotMapped <- direct_bill[!(direct_bill$key_db %in%  df$key_db),]
df <- bind_rows(df,direct_bill_NotMapped)

#Creating a consolidated column for retail and direct billing
df$retail <- ifelse(is.na(df$retail),0,df$retail)
df$retaildirectbilling <- ifelse(is.na(df$retaildirectbilling),0,df$retaildirectbilling)
df$retail <- df$retail+df$retaildirectbilling       

#df$retaildirectbilling <- NULL
df$key_db <- NULL

#Create Column with Value India
df$national <- "INDIA" 

################################################################################################
# GENERATING THE OUTPUT FILE#
################################################################################################
Unique_combi_Power_BI <- data.frame(unique(df[,c("region","state","dealer","dealercode","lob","ppl","pl",
                                                 "vc","tonnage","rnp1","rnp2","tonnageaggregate")]))
#months_2019<-c("12")
months_2020<-c("1","2","3")

# Combi_2019 <- merge(Unique_combi_Power_BI,months_2019,all.x=T)
# Combi_2019 <- setnames(Combi_2019,"y","month")
# Combi_2019$month <- as.numeric(as.character(Combi_2019$month))  
# Combi_2019$years<-as.numeric(2019)
# Combi_2019$key <- paste(Combi_2019$vc, Combi_2019$dealercode,Combi_2019$years,
#                            Combi_2019$month,sep="-")
# nrow(Combi_2019)
# length(unique(Combi_2019$key))
# 
# xx<-data.frame(table(Combi_2019$key))

#write.csv(xx,"xx.csv")
#write.csv(df,"df_check.csv")

Combi_2020<- merge(Unique_combi_Power_BI,months_2020,all.x=T)
Combi_2020 <- setnames(Combi_2020,"y","month")
Combi_2020$month <- as.numeric(as.character(Combi_2020$month))  
Combi_2020$years<- as.numeric(2020)

##Checks for duplicates
nrow(Combi_2020)
length(unique(paste(Combi_2020$vc, Combi_2020$dealercode,Combi_2020$years,
                         Combi_2020$month,sep="-")))


Combi_2019_2020 <- data.frame()
Combi_2019_2020 <-rbind(Combi_2019_2020,Combi_2020)

Combi_2019_2020$startdate<- as.Date(unique(df$startdate), "%m/%d/%Y")
Combi_2019_2020$yearmonth <- paste(Combi_2019_2020$month,"1",Combi_2019_2020$years,sep = "/")
Combi_2019_2020$yearmonth <- as.Date(Combi_2019_2020$yearmonth,"%m/%d/%Y")
Combi_2019_2020$retail <- 0
Combi_2019_2020$retaildirectbilling <- 0
Combi_2019_2020$national <- "INDIA" 


Power_BI_Output<- bind_rows(df[,c("region","state","dealer","dealercode","lob","ppl","pl",
                                  "vc","tonnage","tonnageaggregate","rnp1","rnp2","startdate","retail","retaildirectbilling","years","month",
                                  "yearmonth","national")],Combi_2019_2020)
unique(Power_BI_Output$yearmonth)

Power_BI_Output_1<- Power_BI_Output
Power_BI_Output_1$key <- paste(Power_BI_Output_1$vc, Power_BI_Output_1$dealercode,Power_BI_Output_1$years,
                               Power_BI_Output_1$month,sep="-") #105813
length(unique(Power_BI_Output_1$key))
nrow(Power_BI_Output_1) #207249
#xx <- data.frame(table(Power_BI_Output_1$key))

#Mapping the model category to the output file
Power_BI_Output <- merge(Power_BI_Output, RNP_Mapping[,c("vc","modelcategory")], by="vc", all.x = T)

################################################################################################
# Adding the Assumptions in the main data & Output file#
################################################################################################

#Adding the assumptions in the main data
temp_df <- df #94528

temp_df<-merge(temp_df, Assumptions, by.x="rnp1", by.y="RNP1", all.x=T)
temp_df$rnp1<-ifelse(is.na(temp_df$Merged_RNP)|temp_df$Merged_RNP=="",temp_df$rnp1,temp_df$Merged_RNP)
temp_df$startdate<-if_else(is.na(temp_df$New_Start_date),temp_df$startdate,temp_df$New_Start_date)

###Subsetting the data based on Start Date####
temp_df<-temp_df[which(temp_df$yearmonth>=temp_df$startdate),]

#Adding the assumptions in the output file
Power_BI_Output<-merge(Power_BI_Output, Assumptions[,c("RNP1","Merged_RNP")],
                       by.x="rnp1", by.y="RNP1", all.x=T)

Power_BI_Output$Merged_RNP<-ifelse(is.na(Power_BI_Output$Merged_RNP)|Power_BI_Output$Merged_RNP=="",
                                   Power_BI_Output$rnp1,
                                   Power_BI_Output$Merged_RNP)

################################################################################################
# Top 5 RNP identification and other mapping keys#
################################################################################################

Top_5<- df %>%
  filter(yearmonth %in% tail(sort(unique(df$yearmonth)),4)) %>%
  group_by(rnp1) %>%
  summarise(retail =sum(retail),
            month_count = length(unique(yearmonth)) )

Top_5$rank <- rank(desc(Top_5$retail), ties.method = "first")                   
Top_5$rnppriority<- ifelse(Top_5$rank <=5 & Top_5$month_count > 3 ,"Top 5 RNP",
                           ifelse(Top_5$rank <=15 & Top_5$month_count > 3, "Next 10 RNP","Others"))


###Mapping the rnp priority to the Output file ########### 
Power_BI_Output<- merge(Power_BI_Output, Top_5[,c("rnp1","rnppriority")], by="rnp1", all.x=T)
Power_BI_Output$rnppriority <-ifelse(is.na(Power_BI_Output$rnppriority),"Others",Power_BI_Output$rnppriority)

### Create Keys at National, Region , State and Dealer Level ###
Power_BI_Output$Key_national  <-paste(Power_BI_Output$national, Power_BI_Output$Merged_RNP,Power_BI_Output$yearmonth, sep="-")
Power_BI_Output$Key_region  <-paste(Power_BI_Output$region, Power_BI_Output$Merged_RNP,Power_BI_Output$yearmonth, sep="-")
Power_BI_Output$Key_state <-paste(Power_BI_Output$state, Power_BI_Output$Merged_RNP,Power_BI_Output$yearmonth, sep="-")
Power_BI_Output$Key_dealer    <-paste(Power_BI_Output$dealercode, Power_BI_Output$Merged_RNP,Power_BI_Output$yearmonth, sep="-")

################################################################################################
#Creating List to pass as key in Loop#
################################################################################################
colnames(temp_df)
looplist <- c("national","region") # list of Key columns

#, "state","dealercode",
## Check unique values 
unique(temp_df$tonnage)
unique(temp_df$rnp1)

temp_df <- temp_df #[temp_df$rnp1=="MAV3742LPT68COWL59CR"
#|temp_df$rnp1=="MAV313518LPT56COWL59CR"
#|temp_df$rnp1=="TT4046SIGNA32CAB59CR"
#|temp_df$rnp1=="MCV161913LPT42CAB697",]
#
################################################################################################
#D E F I N E    G L O B A L    P A R A M S#
################################################################################################

#Define Number of points for test and forecast

testPoints = 0  #No of observation for testing
forecastTill = "2020-04-01" #Forecast Till
startPoint = 1  #Details Required to split data set
minObservation = 18 #Minimum observation required to develop model
minObservation_SMA = 3 #Minimum observation required to develop SMA model
minRetail = 45 #Minimum total retail required to develop model

## Set 3 data frames for  Model Output and Model Forecast ####
modelOutput <- data.frame() # create empty data frame for modeloutput
Dropped<- data.frame() # create empty data frame for dropped models
#j=1
## loop for model development starts here ##
for(j in 1:length(looplist)){
  
  temp_df$Key <- paste(temp_df[,looplist[j]], temp_df$rnp1, sep="-") # Create a key for Tonnage across all the 4 levels #
  
  ## Aggregate data at Year-Month & Tonnage Level ####
  df2 <- aggregate(temp_df$retail, list(temp_df$Key, temp_df$yearmonth), sum)
  
  ## Rename the Columns ##
  colnames(df2) <- c("Key", "Month", "Retail")
  unique(df2$Key)
  
  #### Take only those Keys RNP1-Dealer combination where records with Retailagg > 27 #### 
  obs <- df2 %>% group_by(Key) %>% summarise(Records = n(),
                                             TotalRetail = sum(Retail),
                                             Max_date = max(Month),
                                             Min_date = min(Month))
  obs <- as.data.frame(obs)
  obs <- obs[order(obs$Records, decreasing = TRUE),]
  colnames(obs)
  
  obs$Min_month <- paste0(year(obs$Max_date),month(obs$Max_date))
  
  #The Retail must be available in recent months
  minYearRequired <- year(max(obs$Max_date))
  minMonthRequired <- c(paste0("2019",month(max(df$yearmonth))),paste0("2019",month(max(df$yearmonth))-1),
                        paste0("2019",month(max(df$yearmonth))-2))
  
  keyList <- unique(obs[obs$Records >= minObservation & obs$TotalRetail > minRetail & 
                          year(obs$Max_date) >=minYearRequired 
                        & obs$Min_month %in% minMonthRequired ,
                        "Key"])
  
  #Final list of keys for ARIMA Model
  keyList <- keyList[!is.na(keyList)]
  length(keyList)
  length(unique(obs$Key))
  
  
  #List of Keys for SMA 
  SMA_Keys<- data.frame(obs[!(obs$Key %in% keyList), ])
  all_SMA_Keys <- unique(SMA_Keys$Key)
  SMA_Keys <- SMA_Keys[SMA_Keys$Records >= minObservation_SMA  & 
                         year(SMA_Keys$Max_date) >= minYearRequired &
                         SMA_Keys$Min_month %in% minMonthRequired, ]
  
  SMA_Keys_List <- unique(SMA_Keys[!is.na(SMA_Keys$Key), "Key"])
  
  #List of Keys for No Model
  droppd_Keys <- all_SMA_Keys[!all_SMA_Keys %in% SMA_Keys_List  ]
  droppd_Keys <- data.frame(Keys = droppd_Keys)
  Dropped <- bind_rows(Dropped, droppd_Keys)
  
  
  ##########   M O D E L    D E V E L O P M E N T ##############
  
  #### Run a loop with all the Key list for 3 Models  ####
  
  for(i in  1:length(keyList)){
    
    print(paste("Iteration Number: ", i)) ### Start iteration
    dta <- df2[df2$Key == keyList[i], c(2,3)] ### subset df2 basis Key one by one
    #dta$Month <- as.Date(dta$Month,"%m/%d/%Y") ### set Month as as.Date
    startMonthNew <- as.Date(min(dta$Month)) ### Take min date
    endMonthNew <- as.Date(max(dta$Month)) ### Take max date
    dummyDf <- data.frame(Month = seq(startMonthNew, endMonthNew, by = "month"))### create a dataframe with seq month for each key
    dummyDf$Retail <- c() ### Set Blank Retail column
    dummyDf$Retail[match(dta$Month, dummyDf$Month)] <- dta$Retail 
    dummyDf$Retail[-match(dta$Month, dummyDf$Month)] <- 0 ### Set points to 0 where month is not available  
    startYear = year(dummyDf[startPoint,1]) ## Start year for each series
    startMonth =  month(dummyDf[startPoint,1]) ## Start month for each series
    endYear = year(dummyDf[nrow(dummyDf),1]) ## start year for each series
    endMonth =  month(dummyDf[nrow(dummyDf),1]) ## start month for each series
    
    #To find number of forecast points
    tempDate <- max(seq(max(dummyDf$Month), by="month", length.out = 2))
    forecastPoints <- length(seq(tempDate, as.Date(forecastTill), by="month"))
    
    #Actual without Imputation
    actual = dummyDf$Retail
    
    #Average Imputation (Imputation to bve done for the month of April 2017)
    aprImpute <- mean(dummyDf[which(dummyDf$Month %in% as.Date(c("2017-05-01", "2017-06-01", "2017-07-01"))), "Retail"])
    dummyDf[dummyDf$Month == "2017-04-01", "Retail"] <- round(aprImpute)
    
    
    #Imputation of Aug and Sept 2018
    #dummyDf[dummyDf$Month == "2018-03-01", "Retail"] <- round(dummyDf[dummyDf$Month == "2018-03-01", "Retail"]*0.6,0) #50%
    #dummyDf[dummyDf$Month == "2019-10-01", "Retail"] <- round(dummyDf[dummyDf$Month == "2019-10-01", "Retail"]*1.1,0) #50%
    #dummyDf[dummyDf$Month == "2019-11-01", "Retail"] <- round(dummyDf[dummyDf$Month == "2019-11-01", "Retail"]*1.1,0) #50%
    
    #Splitting Dataset into Train and Test
    train = ts(dummyDf[startPoint:(nrow(dummyDf)-testPoints), 2], start = c(startYear,startMonth), frequency = 12) ###Create Train set
    test = tail(dummyDf[,2], testPoints) ### Create test Set
    
    #############################################################################################
    #                   Model Development : Based Arima
    #############################################################################################
    
    #Run Models ::: Auto Arima Fit#
    ar_m1 <- auto.arima(train, stepwise =F ,approximation =F ,trace = T)
    #coef(ar_m1)
    #summary(ar_m1)
    pred_ar_m1 <- forecast(ar_m1, h=forecastPoints)
    #plot(pred_ar_m1)
    #checkresiduals(ar_m1)
    
    pred_for_valid_m1 <- pred_ar_m1$mean[1:testPoints] # get predicted values
    forecasted_m1 <- round(pred_ar_m1$mean) # round the forecasted values
    
    sample_num<- sample(1:5,length(forecasted_m1),replace = T)
    
    if(var(tail(forecasted_m1,forecastPoints-2))==0){
      forecasted_m1 <- cbind(sample_num+forecasted_m1)
    } else {
      forecasted_m1 <- forecasted_m1
    }
    
    date <- seq(startMonthNew, by = "month", length.out = length(train)+ forecastPoints)
    actual <- append(actual, rep(NA, length(date)-length(actual)))
    
    series <- append(round(ar_m1$fitted), round(forecasted_m1)) # append the fitted and forecasted values
    
    # Model Output #
    outputDf <- data.frame(Key = rep(keyList[i], length(date)),
                           Month = date,
                           Actual = actual,
                           Predicted  = series,
                           Comment = "No Transformation",
                           #Var= var(forecasted_m1)
                           StartTime = obs[which(obs$Key ==keyList[i]), "Min_date"])
    
    modelOutput <- bind_rows(modelOutput, outputDf)
    
    #############################################################################################
    #                   Model Development : Based Log & Normalization
    #############################################################################################
    ## Log transform the values ##
    rmPoint <- as.integer(rownames(dummyDf[dummyDf$Retail[1:length(train)]==0,]))
    rmPoint <- rmPoint[rmPoint<= length(train)]
    train_m2 <- train
    train_m2 <- log(train_m2)
    train_m2[rmPoint] <- 0
    
    ## Normalization ##
    min_m2 <- min(train_m2)
    divider_m2 <- (max(train_m2) - min(train_m2))
    train_m2 <- (train_m2 - min_m2)/divider_m2
    
    #Model2
    ar_m2 <- auto.arima(train_m2,stepwise =F ,approximation =F , trace = T)
    #coef(ar_m2)
    #summary(ar_m2)
    pred_ar_m2 <- forecast(ar_m2, h=forecastPoints)
    #plot(pred_ar_m2)
    #checkresiduals(ar_m2)
    pred_for_valid_m2 <- pred_ar_m2$mean[1:testPoints]
    pred_for_valid_m2 <- exp(pred_for_valid_m2*divider_m2 + min_m2)
    
    forecasted_m2 <- pred_ar_m2$mean
    forecasted_m2 <- round(exp(forecasted_m2*divider_m2 + min_m2))
    
    sample_num<- sample(1:5,length(forecasted_m2),replace = T)
    
    if(var(tail(forecasted_m2,forecastPoints-2))==0){
      forecasted_m2 <- cbind(sample_num+forecasted_m2)
    } else {
      forecasted_m2 <- forecasted_m2  
    }                        
    
    
    fitted_value_m2 <- (ar_m2$fitted)
    fitted_value_m2 <- round(exp(fitted_value_m2*divider_m2 + min_m2))
    
    series2 <- append(round(fitted_value_m2), round(forecasted_m2))
    
    # Model Output #
    outputDf <- data.frame(Key = rep(keyList[i], length(date)),
                           Month = date, 
                           Actual = actual,
                           Predicted  = series2,
                           Comment = "LNTransform",
                           #Var= var(forecasted_m2)
                           StartTime = obs[which(obs$Key ==keyList[i]), "Min_date"])
    
    modelOutput <- bind_rows(modelOutput, outputDf)
    
    #############################################################################################
    #                   Model Development : Exponential 
    #############################################################################################
    ### Fit the Model ###
    expo <- ets(train, model = "AAN", damped = TRUE )
    ### Forecast ###
    fc <- forecast(expo, h=forecastPoints)
    ### Checke Accuracy ###
    accuracy(fc)
    ### Fitted values ###
    round(expo$fitted)
    
    forecasted_m3<-round(fc$mean)
    #train
    
    sample_num<- sample(1:5,length(forecasted_m3),replace = T)
    
    if(var(tail(forecasted_m3,forecastPoints-2))==0){
      forecasted_m3 <- cbind(sample_num+forecasted_m3)
    } else {
      forecasted_m3 <- forecasted_m3
    }
    
    series3 <- append(round(expo$fitted), forecasted_m3)
    
    # Model Output #
    outputDf <- data.frame(Key = rep(keyList[i], length(date)),
                           Month = date,
                           Actual = actual,
                           Predicted  = series3,
                           Comment = "Exponential",
                           #Var= var(fc$mean),
                           StartTime = obs[which(obs$Key ==keyList[i]), "Min_date"])
    
    modelOutput <- bind_rows(modelOutput, outputDf)
    
    
    
  }  
  
  #If there are no Keys for the SMA it will skip this part
  if(length(SMA_Keys_List) == 0) {
    next
  } else {
    
    #############################################################################################
    #                   Model Development : Simple Moving Average 
    #############################################################################################
    
    
    for(k in  1:length(SMA_Keys_List)){
      
      print(paste("SMA Iteration Number: ", k)) ### Start iteration
      dta_SMA <- df2[df2$Key == SMA_Keys_List[k], c(2,3)] ### subset df2 basis Key one by one
      #dta$Month <- as.Date(dta$Month,"%m/%d/%Y") ### set Month as as.Date
      startMonthNew_SMA <- as.Date(min(dta_SMA$Month)) ### Take min date
      endMonthNew_SMA <- as.Date(max(dta_SMA$Month)) ### Take max date
      dummyDf_SMA <- data.frame(Month = seq(startMonthNew_SMA, endMonthNew_SMA, by = "month"))### create a dataframe with seq month for each key
      dummyDf_SMA$Retail <- c() ### Set Blank Retail column
      dummyDf_SMA$Retail[match(dta_SMA$Month, dummyDf_SMA$Month)] <- dta_SMA$Retail
      dummyDf_SMA$Retail[-match(dta_SMA$Month, dummyDf_SMA$Month)] <- 0 ### Set points to 0 where month is not available
      startYear_SMA = year(dummyDf_SMA[startPoint,1]) ## Start year for each series
      startMonth_SMA =  month(dummyDf_SMA[startPoint,1]) ## Start month for each series
      endYear_SMA = year(dummyDf_SMA[nrow(dummyDf_SMA),1]) ## start year for each series
      endMonth_SMA =  month(dummyDf_SMA[nrow(dummyDf_SMA),1]) ## start month for each series
      
      #To find number of forecast points
      tempDate_SMA <-  max(seq(max(dummyDf_SMA$Month), by="month", length.out = 2))
      forecastPoints_SMA <- length(seq(tempDate_SMA, as.Date(forecastTill), by="month"))
      
      
      #Average Imputation (Imputation to bve done for the month of April 2017)
      aprImpute_SMA <- mean(dummyDf_SMA[which(dummyDf_SMA$Month %in% as.Date(c("2017-05-01", "2017-06-01", "2017-07-01"))), "Retail"])
      dummyDf_SMA[dummyDf_SMA$Month == "2017-04-01", "Retail"] <- round(aprImpute_SMA)
      
      #Imputation of Recent Months
      #dummyDf_SMA[dummyDf_SMA$Month == "2018-03-01" , "Retail"] <- round(dummyDf_SMA[dummyDf_SMA$Month == "2018-03-01", "Retail"]*0.6,0) #10%
      #dummyDf_SMA[dummyDf_SMA$Month == "2019-10-01" , "Retail"] <- round(dummyDf_SMA[dummyDf_SMA$Month == "2019-10-01", "Retail"]*1.1,0) #10%
      #dummyDf_SMA[dummyDf_SMA$Month == "2019-11-01" , "Retail"] <- round(dummyDf_SMA[dummyDf_SMA$Month == "2019-11-01", "Retail"]*1.1,0) #10%
      
      #Actual without Imputation
      actual_SMA = dummyDf_SMA$Retail
      
      ##Adding a small factor to the values to remove the constant number effect 
      dummyDf_SMA$Retail<-sapply(dummyDf_SMA$Retail, function(x) (x + runif(1)))  
      
      
      #Splitting Dataset into Train and Test
      train_SMA = ts(dummyDf_SMA[startPoint:(nrow(dummyDf_SMA)-testPoints), 2], start = c(startYear_SMA,startMonth_SMA), frequency = 12) ###Create Train set
      test_SMA = tail(dummyDf_SMA[,2], testPoints) ### Create test Set
      
      date_SMA <- seq(startMonthNew_SMA, by = "month", length.out = length(train_SMA)+ forecastPoints_SMA)
      
      #Adding "NA" values to the actual retail series to make its length consistent
      actual_SMA <- append(actual_SMA, rep(NA, length(date_SMA)-length(actual_SMA)))
      
      ### Fit the Model ###
      SMA <- sma(train_SMA, order=2)
      ### Forecast ###
      SMA_fc <- forecast(SMA, h=forecastPoints_SMA)
      
      #sample_SMA<- sample(1:5,length(SMA_fc$forecast),replace = T)
      
      # if(var(tail(SMA_fc$forecast,length(SMA_fc$forecast)))==0){
      #   SMA_fc  <- cbind(SMA_fc$forecast+sample_SMA) 
      # } else {
      #   SMA_fc
      # }
      # 
      SMA_fc <- round(append(SMA$fitted, SMA_fc$forecast),0)
      
      
      # Model Output #
      outputDf <- data.frame(Key = rep(SMA_Keys_List[k], length(date_SMA)),
                             Month = date_SMA,
                             Actual = actual_SMA,
                             Predicted  = SMA_fc,
                             Comment = "Simple Moving Average",
                             StartTime = SMA_Keys[which(SMA_Keys$Key == SMA_Keys_List[k]), "Min_date"])
      
      modelOutput <- bind_rows(modelOutput, outputDf)
      
      #############################################################################################
      #                   Model Development : Random Walk 
      #############################################################################################
      ### Fit the Model ###
      rdmwalk <- rwf(train_SMA, h=forecastPoints_SMA)
      
      ### Checke Accuracy ###
      accuracy(rdmwalk)
      ### Fitted values ###
      round(rdmwalk$fitted)
      
      forecasted_m4<-round(rdmwalk$mean)
      #train
      
      #sample_num<- sample(1:5,length(forecasted_m4),replace = T)
      
      # if(var(tail(forecasted_m4,forecastPoints-2))==0){
      #   forecasted_m4 <- cbind(sample_num+forecasted_m4)
      # } else {
      #   forecasted_m4 <- forecasted_m4  
      # }  
      
      series4 <- append(round(rdmwalk$fitted), forecasted_m4)
      
      # Model Output #
      outputDf <- data.frame(Key = rep(SMA_Keys_List[k], length(date_SMA)),
                             Month = date_SMA, 
                             Actual = actual_SMA,
                             Predicted  = series4,
                             Comment = "Random Walk",
                             #Var= var(fc$mean),
                             StartTime = SMA_Keys[which(SMA_Keys$Key ==SMA_Keys_List[k]), "Min_date"])
      
      modelOutput <- bind_rows(modelOutput, outputDf)
      
      #############################################################################################
      #                   Model Development : Random Walk With Drift 
      #############################################################################################
      ### Fit the Model ###
      rdmwalk_drift <- rwf(train_SMA, drift = TRUE, h=forecastPoints_SMA)
      
      ### Checke Accuracy ###
      accuracy(rdmwalk_drift)
      ### Fitted values ###
      round(rdmwalk_drift$fitted)
      
      forecasted_m5<-round(rdmwalk_drift$mean)
      #train
      
      # sample_num<- sample(1:5,length(forecasted_m5),replace = T)
      # 
      # if(var(tail(forecasted_m5,forecastPoints-2))==0){
      #   forecasted_m5 <- cbind(sample_num+forecasted_m5)
      # } else {
      #   forecasted_m5 <- forecasted_m5  
      # }  
      
      series5 <- append(round(rdmwalk_drift$fitted), forecasted_m5)
      
      # Model Output #
      outputDf <- data.frame(Key = rep(SMA_Keys_List[k], length(date_SMA)),
                             Month = date_SMA, 
                             Actual = actual_SMA,
                             Predicted  = series5,
                             Comment = "Random Walk With Drift",
                             #Var= var(fc$mean),
                             StartTime = SMA_Keys[which(SMA_Keys$Key ==SMA_Keys_List[k]), "Min_date"])
      
      modelOutput <- bind_rows(modelOutput, outputDf)
      
      
      #############################################################################################
      #                   Model Development : Naive Forecast
      #############################################################################################
      ### Fit the Model ###
      nf <- naive(train_SMA, h=forecastPoints_SMA)
      
      ### Checke Accuracy ###
      accuracy(nf)
      ### Fitted values ###
      round(nf$fitted)
      
      forecasted_m6<-round(nf$mean)
      #train
      
      #sample_num<- sample(1:5,length(forecasted_m6),replace = T)
      
      # if(var(tail(forecasted_m6,forecastPoints-2))==0){
      #   forecasted_m6 <- cbind(sample_num+forecasted_m6)
      # } else {
      #   forecasted_m6 <- forecasted_m6  
      # }  
      
      series6 <- append(round(nf$fitted), forecasted_m6)
      
      # Model Output #
      outputDf <- data.frame(Key = rep(SMA_Keys_List[k], length(date_SMA)),
                             Month = date_SMA, 
                             Actual = actual_SMA,
                             Predicted  = series6,
                             Comment = "Naive Forecast",
                             #Var= var(fc$mean),
                             StartTime = SMA_Keys[which(SMA_Keys$Key ==SMA_Keys_List[k]), "Min_date"])
      
      modelOutput <- bind_rows(modelOutput, outputDf)
      
      
      
      #############################################################################################
      #                   Model Development : Naive with Seasonality
      #############################################################################################
      ### Fit the Model ###
      snf <- rwf(train_SMA,  h=forecastPoints_SMA)
      
      ### Checke Accuracy ###
      accuracy(snf)
      ### Fitted values ###
      round(snf$fitted)
      
      forecasted_m7<-round(snf$mean)
      #train
      
      # sample_num<- sample(1:5,length(forecasted_m7),replace = T)
      # 
      # if(var(tail(forecasted_m7,forecastPoints-2))==0){
      #   forecasted_m7 <- cbind(sample_num+forecasted_m7)
      # } else {
      #   forecasted_m7 <- forecasted_m7  
      # }  
      
      series7 <- append(round(snf$fitted), forecasted_m7)
      
      # Model Output #
      outputDf <- data.frame(Key = rep(SMA_Keys_List[k], length(date_SMA)),
                             Month = date_SMA, 
                             Actual = actual_SMA,
                             Predicted  = series7,
                             Comment = "Naive With Seasonality",
                             #Var= var(fc$mean),
                             StartTime = SMA_Keys[which(SMA_Keys$Key ==SMA_Keys_List[k]), "Min_date"])
      
      modelOutput <- bind_rows(modelOutput, outputDf)
      
      
    }
  }
  
}
########## SPLITING KEY COLUMN ############

modelOutput$Key2 <- modelOutput$Key
modelOutput <- cSplit(modelOutput, "Key2", sep="-", type.convert = FALSE) # split the key
modelOutput <- as.data.frame(modelOutput)

#Rename new columns
keyCol_index <- which(colnames(modelOutput) %in% c("Key2_1", "Key2_2"))
colnames(modelOutput)[keyCol_index] <- c("SubLevel", "RNP1")
colnames(modelOutput)

unique(modelOutput$SubLevel)

modelOutput$model_Level <- ifelse(modelOutput$SubLevel %in% df$national, "Overall", 
                                  ifelse(modelOutput$SubLevel %in% unique(df$region), "Region",
                                         ifelse(modelOutput$SubLevel %in% unique(df$state), "State", "Dealer")))
unique(modelOutput$model_Level)

############# SUMMARY DF ###############

modelOutput$modelKey <- paste(modelOutput$Key, modelOutput$Comment, sep="-")
model_List <- unique(modelOutput$modelKey)
summaryDf <- data.frame()

for(i in 1:length(model_List)){
  
  temp_modelDf <- modelOutput[modelOutput$modelKey == model_List[i],]
  temp_modelDf$Error <- temp_modelDf$Predicted - temp_modelDf$Actual
  temp_modelDf$ErrorPercent <- temp_modelDf$Error/temp_modelDf$Actual
  
  temp_modelDf[temp_modelDf$Actual==0 & !is.na(temp_modelDf$Actual), "ErrorPercent"] <- 0
  
  last4Month_mape <- mean(abs(tail(temp_modelDf[!is.na(temp_modelDf$ErrorPercent), "ErrorPercent"],4))) ## calculate the last 4 months mape
  last4Month_mape <- round(last4Month_mape*100,2)
  
  
  overall_mape <- mean(abs(temp_modelDf$ErrorPercent), na.rm = TRUE) # calcualte the overall mape 
  overall_mape <- round(overall_mape*100,2)
  
  
  last4months_rmse <- mean(sqrt(abs(tail(temp_modelDf[!is.na(temp_modelDf$Error), "Error"],4)))) # calcualte the last 4 months rmse
  last4months_rmse <- round(last4months_rmse,2)
  
  
  temp_modelDf$FY <- ifelse(month(temp_modelDf$Month) > 3, year(temp_modelDf$Month)+1, year(temp_modelDf$Month))
  temp_modelDf$FY_name <- paste(temp_modelDf$FY -1, temp_modelDf$FY, sep="-")
  
  #Var<- unique(temp_modelDf$Var)
  
  #Actual_2018_2019 <- sum(temp_modelDf[temp_modelDf$FY_name == "2018-2019", "Actual"], na.rm = TRUE)
  #Forecast_2019_2020 <- sum(temp_modelDf[temp_modelDf$FY_name == "2019-2020", "Predicted"], na.rm = TRUE)
  
  temp_Summary_df <- data.frame(Key = model_List[i],
                                #Var = Var,
                                last4Month_mape = last4Month_mape,
                                overall_mape = overall_mape, 
                                #Actual_2018_2019 = Actual_2018_2019,
                                #Forecast_2019_2020 = Forecast_2019_2020,
                                last4months_rmse = last4months_rmse)
  summaryDf <- bind_rows(summaryDf, temp_Summary_df)
  
}

### Get the Summary ###

summaryDf$Key2 <- summaryDf$Key
summaryDf <-  cSplit(summaryDf, 'Key2', sep =  "-", type.convert=FALSE)
summaryDf <-  as.data.frame(summaryDf)

keyCol_index <- which(colnames(summaryDf) %in% c("Key2_1", "Key2_2", "Key2_3"))
colnames(summaryDf)[keyCol_index] <- c("SubLevel","RNP1","ModelType")
colnames(summaryDf)
summaryDf$model_Level <- ifelse(summaryDf$SubLevel %in% df$national, "Overall", 
                                ifelse(summaryDf$SubLevel %in% unique(df$region), "Region",
                                       ifelse(summaryDf$SubLevel %in% unique(df$state), "State", "Dealer")))
summaryDf$temp_Key <- paste(summaryDf$SubLevel, summaryDf$RNP1, sep="-")

#### Selecting and Removing models that give negative forecasts ####

Negative<- modelOutput %>%
  group_by(RNP1, SubLevel, Comment) %>%
  summarise(Total_Predicted= sum(Predicted,na.rm = T),Total_Abs_Predicted= sum(abs(Predicted),na.rm = T)) 

Negative$Flag <- ifelse(Negative$Total_Predicted<Negative$Total_Abs_Predicted,1,0)
Negative$Key <- paste(Negative$SubLevel,Negative$RNP1, Negative$Comment,sep = "-")


summaryDf_1 <- sqldf('select * from summaryDf 
                     where Key in(
                     select Key from Negative where Flag = 0)')

#### Selecting Best Model basis Mape calcualted from last 4 months ####

summaryDf_1$minMape <- ave(summaryDf_1$last4Month_mape, summaryDf_1$temp_Key, FUN = min)
best_summaryDf <- summaryDf_1[summaryDf_1$last4Month_mape==summaryDf_1$minMape,]

## There are models for which the Mape, rmse are same ###
## Finding cases where mape and rmse are same ####

Multiple_cnt<-data.frame(table(best_summaryDf$temp_Key))
Multiple_cnt<-Multiple_cnt[which(Multiple_cnt$Freq>1) ,] 

## Subsetting such cases ###
best_summaryDf_1<-subset(best_summaryDf, (best_summaryDf$temp_Key %in% Multiple_cnt$Var1))

## Subsetting the normal cases ###
best_summaryDf<-subset(best_summaryDf, !(best_summaryDf$temp_Key %in% Multiple_cnt$Var1))

## Selecting "No Transformation" by default followed by "Ln Transform" in cases where mape and rmse are same 

best_summaryDf_1$Model_Code<-ifelse(best_summaryDf_1$ModelType=="No Transformation",1,
                                    ifelse(best_summaryDf_1$ModelType=="LNTransform",2,
                                           ifelse(best_summaryDf_1$ModelType=="Exponential",3,
                                                  ifelse(best_summaryDf_1$ModelType=="Simple Moving Average",4,       
                                                         ifelse(best_summaryDf_1$ModelType=="Random Walk",5,
                                                                ifelse(best_summaryDf_1$ModelType=="Random Walk With Drift",6,
                                                                       ifelse(best_summaryDf_1$ModelType=="Naive Forecast",7,8)))))))

best_summaryDf_1<-best_summaryDf_1[order(best_summaryDf_1$temp_Key,best_summaryDf_1$Model_Code),]

best_summaryDf_1$minmodel <- ave(best_summaryDf_1$Model_Code, best_summaryDf_1$temp_Key, FUN = min)

best_summaryDf_1 <- best_summaryDf_1[best_summaryDf_1$Model_Code==best_summaryDf_1$minmodel,]

best_summaryDf_1<-best_summaryDf_1[,!(colnames(best_summaryDf_1) %in% c("minmodel","Model_Code"))]

### Appending all the models ###
best_summaryDf<-rbind(best_summaryDf,best_summaryDf_1)
best_ModelOutput <- modelOutput[modelOutput$modelKey %in% unique(best_summaryDf$Key),]

###Rearranging Columns for best Model
colnames(best_summaryDf)
reqVar1 <- c("model_Level", "SubLevel", "RNP1", "ModelType", "last4Month_mape", "overall_mape", 
             "last4months_rmse")
best_summaryDf <- best_summaryDf[, reqVar1]

colnames(best_ModelOutput)
reqVar2 <- c("model_Level", "SubLevel", "RNP1", "Month", "Actual", "Predicted", "Comment", "StartTime")
best_ModelOutput <- best_ModelOutput[, reqVar2]

#Overall Dump 
summaryDf <- summaryDf[, reqVar1]
modelOutput <- modelOutput[, reqVar2]


##########Mapping into the Power BI Output File for Overall and Region########### 

best_ModelOutput$Key<-paste(best_ModelOutput$SubLevel,best_ModelOutput$RNP1,best_ModelOutput$Month,sep="-")

Power_BI_Output<-merge(Power_BI_Output,best_ModelOutput[,c("Key","Predicted")], by.x = "Key_national",by.y = "Key",all.x = T )
colnames(Power_BI_Output)[which(names(Power_BI_Output) == "Predicted")] <- "Overall_RNP1"

Power_BI_Output<-merge(Power_BI_Output,best_ModelOutput[,c("Key","Predicted")], by.x = "Key_region",by.y = "Key",all.x = T )
colnames(Power_BI_Output)[which(names(Power_BI_Output) == "Predicted")] <- "Region_RNP1"

#Power_BI_Output<-merge(Power_BI_Output,best_ModelOutput[,c("Key","Predicted")], by.x = "Key_state",by.y = "Key",all.x = T )
#colnames(Power_BI_Output)[which(names(Power_BI_Output) == "Predicted")] <- "State_RNP1"

#Power_BI_Output<-merge(Power_BI_Output,best_ModelOutput[,c("Key","Predicted")], by.x = "Key_dealer",by.y = "Key",all.x = T )
#colnames(Power_BI_Output)[which(names(Power_BI_Output) == "Predicted")] <- "Dealer_RNP1"


## Adjusting forecast post model development
# Power_BI_Output_March <- Power_BI_Output %>%
#                           filter(Power_BI_Output$yearmonth=="2020-03-01") %>%
#                           mutate(Overall_RNP1 = round(Overall_RNP1*0.9),
#                                  Region_RNP1 = round(Region_RNP1*0.9))  
# #
# Power_BI_Output <- subset(Power_BI_Output, !(Power_BI_Output$yearmonth=="2020-03-01"))
# Power_BI_Output <- rbind(Power_BI_Output,Power_BI_Output_March)  

#################################################################################################################


########Splitting the series#########
#getting the contribution of each of the merged RNP in recent 3 months of 2019
#temp_df2<-df[df$years=="2019",]
temp_df2<-df[df$yearmonth %in% tail(sort(unique(df$yearmonth)),3),]

temp_df2<-aggregate(retail~rnp1,temp_df2,sum)

temp_df2<- sqldf('select * from temp_df2
                 where rnp1  in 
                 (select RNP1 from Assumptions where Merged_RNP not in (""))')

temp_df2<-merge(temp_df2, Assumptions[,c("RNP1","Merged_RNP")], by.x = "rnp1", by.y="RNP1", all.x=T)

##Generating the ratio to split the forecasted numbers
temp_df2 <- temp_df2 %>%
  group_by(Merged_RNP) %>%
  mutate(ratio= retail/sum(retail))

#######Mappng the rnp contribution ratio into the output file #######################

Power_BI_Output<-merge(Power_BI_Output, temp_df2[,c("rnp1","ratio")], by="rnp1",
                       all.x=T)

Power_BI_Output$Overall_RNP1<-ifelse(is.na(Power_BI_Output$ratio),Power_BI_Output$Overall_RNP1,
                                     round(Power_BI_Output$Overall_RNP1*Power_BI_Output$ratio,0))

Power_BI_Output$Region_RNP1<-ifelse(is.na(Power_BI_Output$ratio),Power_BI_Output$Region_RNP1,
                                    round(Power_BI_Output$Region_RNP1*Power_BI_Output$ratio,0))


#Power_BI_Output$State_RNP1<-ifelse(is.na(Power_BI_Output$ratio),Power_BI_Output$State_RNP1,
#                                    round(Power_BI_Output$State_RNP1*Power_BI_Output$ratio,0))


#Power_BI_Output$Dealer_RNP1<-ifelse(is.na(Power_BI_Output$ratio),Power_BI_Output$Dealer_RNP1,
#                                  round(Power_BI_Output$Dealer_RNP1*Power_BI_Output$ratio,0))

################################################################################################
################## State and Dealer Level Distribution ##############################
################################################################################################
###Region forcasts#########

Region_Forecast <- Power_BI_Output %>%
  group_by(rnp1, region, yearmonth) %>%  
  summarise(Region_RNP1 = mean(Region_RNP1))

Region_Forecast$key<- paste(Region_Forecast$region,Region_Forecast$rnp1,
                            Region_Forecast$yearmonth,sep="-")

###State Contributions 

State_Contri<-df %>%
  filter(yearmonth %in% tail(sort(unique(df$yearmonth)),3)) %>%
  group_by(rnp1,region,state) %>%
  summarise(retail= sum(retail))

State_Contri<-State_Contri %>%
  group_by(rnp1,region) %>%  
  mutate(state_contri= retail/sum(retail),
         key1= paste(rnp1,state,sep="-"))

State_Predictions<-data.frame(unique(Power_BI_Output[,c("rnp1","region","state","yearmonth")])) 

###Mapping the state contributions
State_Predictions$key1<-paste(State_Predictions$rnp1,State_Predictions$state,sep="-")
State_Predictions <- merge(State_Predictions, State_Contri[,c("key1","state_contri")],
                           by="key1", all.x=T)


State_Predictions$key2<-paste(State_Predictions$region,State_Predictions$rnp1,
                              State_Predictions$yearmonth,sep="-")

###Mapping the region forecasts
State_Predictions <- merge(State_Predictions, Region_Forecast[,c("key","Region_RNP1")],
                           by.x="key2",by.y="key", all.x=T)

#Multiplying the forecast with the ratio
State_Predictions$Predicted <- round(State_Predictions$state_contri * State_Predictions$Region_RNP1,0)
#State_Predictions$Predicted <- ifelse(is.na(State_Predictions$Predicted),0,State_Predictions$Predicted)

#Mapping State forecasts into into Power_BI
Power_BI_Output$Key_state <-paste(Power_BI_Output$state, Power_BI_Output$rnp1,Power_BI_Output$yearmonth, sep="-")
State_Predictions$key3<-paste(State_Predictions$state,State_Predictions$rnp1, State_Predictions$yearmonth, sep="-")

Power_BI_Output<-merge(Power_BI_Output,State_Predictions[,c("key3","Predicted")], by.x = "Key_state",by.y = "key3",all.x = T )
colnames(Power_BI_Output)[which(names(Power_BI_Output) == "Predicted")] <- "State_RNP1"

################################################################################################
###Dealer Contributions 
################################################################################################
Dealer_Contri<-df %>%
  filter(yearmonth %in% tail(sort(unique(df$yearmonth)),6)) %>%
  group_by(rnp1,region,dealer,dealercode) %>%
  summarise(retail= sum(retail))

Dealer_Contri<-Dealer_Contri %>%
  group_by(rnp1,region) %>%  
  mutate(dealer_contri= retail/sum(retail),
         key1= paste(rnp1,dealercode,sep="-"))

Dealer_Predictions<-data.frame(unique(Power_BI_Output[,c("rnp1","region","state","dealer",
                                                         "dealercode","yearmonth")])) 

###Mapping the Dealer contributions
Dealer_Predictions$key1<-paste(Dealer_Predictions$rnp1,Dealer_Predictions$dealercode,sep="-")

Dealer_Predictions <- merge(Dealer_Predictions, Dealer_Contri[,c("key1","dealer_contri")],
                            by="key1", all.x=T)


Dealer_Predictions$key2<-paste(Dealer_Predictions$region,Dealer_Predictions$rnp1,
                               Dealer_Predictions$yearmonth,sep="-")

###Mapping the region forecasts
Dealer_Predictions <- merge(Dealer_Predictions, Region_Forecast[,c("key","Region_RNP1")],
                            by.x="key2",by.y="key", all.x=T)

#Multiplying the forecast with the ratio
Dealer_Predictions$Predicted <- round(Dealer_Predictions$dealer_contri * Dealer_Predictions$Region_RNP1,0)
#Dealer_Predictions$Predicted <- ifelse(is.na(Dealer_Predictions$Predicted),0,Dealer_Predictions$Predicted)

#Mapping State forecasts into into Power_BI
Power_BI_Output$Key_dealer    <-paste(Power_BI_Output$dealercode, Power_BI_Output$rnp1,Power_BI_Output$yearmonth, sep="-")
Dealer_Predictions$key3<-paste(Dealer_Predictions$dealercode,Dealer_Predictions$rnp1,Dealer_Predictions$yearmonth, sep="-")

#ac <- merge(Power_BI_Output,Dealer_Predictions[,c("key3","Predicted")], by.x = "Key_dealer",by.y = "key3",all.x = T )

Power_BI_Output<-merge(Power_BI_Output,Dealer_Predictions[,c("key3","Predicted")], by.x = "Key_dealer",by.y = "key3",all.x = T )
colnames(Power_BI_Output)[which(names(Power_BI_Output) == "Predicted")] <- "Dealer_RNP1"

################################################################################################
####### RNP2 Predictions at Overall Level
################################################################################################

Retail_Last_Month <- Power_BI_Output %>%
  filter(yearmonth %in% tail(sort(unique(df$yearmonth)),3))  %>%
  group_by(rnp1,rnp2) %>%
  summarize(retail = sum(retail))

Retail_Last_Month <- Retail_Last_Month %>%
  group_by(rnp1) %>%
  mutate(retail_contri = retail/sum(retail))  

RNP2_Predictions <- data.frame(unique(Power_BI_Output[,c("rnp1","rnp2","yearmonth","Overall_RNP1")]))
RNP2_Predictions <- merge(RNP2_Predictions, Retail_Last_Month[,c("rnp1","rnp2","retail_contri")],by=c("rnp1","rnp2"),all.x=T)


RNP2_Predictions$Overall_RNP2 <- round(RNP2_Predictions$Overall_RNP1 * RNP2_Predictions$retail_contri,0)


Power_BI_Output <- merge(Power_BI_Output,RNP2_Predictions[,c("rnp1","rnp2","yearmonth","Overall_RNP2")],
                         by=c("rnp1","rnp2","yearmonth"), all.x=T)


#write.csv(Power_BI_Output,"Cargo_Forecast_IAL_RNP1_RNP2_13_11.csv",na = "",row.names = F)

### Create a work book and save the output to the work sheets ###
library(openxlsx)
wb <- createWorkbook()
addWorksheet(wb, "Power_BI_Output")
addWorksheet(wb, "best_summaryDf")
addWorksheet(wb, "modelOutput")
writeData(wb, 1, Power_BI_Output)
writeData(wb, 2, best_summaryDf)
writeData(wb, 3, modelOutput)
v=0
v = v+1
fileName  = paste("MHCV_Automation_with_Dec_23_No_Val",".xlsx", sep="")
folder = paste0(getwd(),"/Output_RNP1/")
dir.create(path= folder)
path = paste(folder, fileName, sep="")
saveWorkbook(wb, file = path, overwrite = TRUE)
detach(package:openxlsx)

b<-Sys.time()
print(b-a) ### Note End time


################# End of the Code #####################
