############## Region RNP level ################
############## PIPELINE  FORECASTING :: Region LEVEL ##########
###-------------------------### At c1  ###-------------------------##

## read File ##
## Main Opty iD level input file ##
## Use the below line 9-12 when importing data directly form S3 or else comment it out ##

# library(aws.s3)
# Sys.setenv("AWS_ACCESS_KEY_ID" = "XXXXXXX")
# Sys.setenv("AWS_SECRET_ACCESS_KEY" = "XXXXXX")
# Sys.setenv("AWS_DEFAULT_REGION" = "XXXXXX)

#getwd()
##source("MHCV_Pipeline_DataPrep_Prediction_v2.R")
#f<-fread("Grannular_Tonnage_Sheet_Nov.csv")

setwd("/home/ankit/")
rnp<-read.csv("Grannular_Tonnage_Sheet_MHCV_20200311.csv")
Master_data<-read.csv("Master_Data.csv")
## Start Date ##
Start_Date<-"2016-04-01"
## Current Date -1 ##
End_Date<-as.character(Sys.Date()-1)

## Read the Prepared data ##
Master_data$C1_Date<-as.Date(Master_data$C1_Date,format="%m/%d/%Y")
Master_data<-Master_data[which(Master_data$C1_Date>='2016-04-01'),]

df <- Master_data
#s3read_using(FUN = read.csv, object = "xxxx.csv", bucket = "xxxx")
#View(df)
library(lubridate)
sum(df$Invoice_Quantity)##436413
sum(df$Quantity)##1359812
#df$C1_Date<-as.Date(df$C1_Date,format="%m/%d/%Y")

## Map RNP1 to the main table ##

library(dplyr)
rnp$RNP1<-tolower(rnp$RNP1)
rnp<-rnp[,c('VC','RNP1')]
rnp<-rnp[which(rnp$VC!=''),]
rnp<-rnp%>%distinct(VC, RNP1, .keep_all = TRUE)

library(sqldf)
sqldf("select distinct VC,count(RNP1) from rnp group by VC having count(RNP1)>1")

df<-merge(df,rnp[,c('VC','RNP1')],by=c("VC"),all.x=TRUE)

nrow(df[is.na(df$RNP1),])##45053
sqldf("select sum(Invoice_Quantity) from df where RNP1 is null")### 10691

df1<-df[which(df$RNP1!=''),]
sqldf("select sum(Invoice_Quantity) from df1 where RNP1 is null")###NA

library(sqldf)
colnames(df1)
df2<-sqldf("select distinct Region,C1_Date,RNP1,sum(Quantity) as Inv_count from df1 group by Region,C1_Date,RNP1")
sum(df2$Inv_count)##1343258

## Drop records where Region is null ##

nrow(df2[which(df2$Region==""),])##340
df2<-df2[which(df2$Region!=""),]

### Write c1 aggregate date ###
#setwd("C:\Users\ankit.patel1\Desktop\Pipeline")
#write.csv(df2,"C:\\Users\\ankit.patel1\\Desktop\\Pipeline\\c1_agg_Region_July2019_1.csv")

## Read Date file ##
### Change the Month File (Seq of Dates) as per the period of Analysis ###
df3<-as.data.frame(seq(from=as.Date(Start_Date),to=as.Date(End_Date),by=1))
colnames(df3)<-c('Date')
# df3<-read.csv("C:\\Users\\ankit.patel1\\Desktop\\Pipeline\\Date_Month.csv")
# df3<-read.csv("C:\\Users\\ankit.patel1\\Desktop\\Pipeline\\Date_Month_1.csv")
# df3<-read.csv("C:\\Users\\ankit.patel1\\Desktop\\Pipeline\\Date_Month_2.csv")

### take columns RNP1 and rEGION ###
#colnames(df1)[3]<-"RNP1"
df4<-df1[,c("Region","RNP1")]

library(dplyr)
df5<-df4 %>% distinct(Region, RNP1, .keep_all = TRUE)
unique(df5$RNP1)##300

## drop blanks from RNP1 ##

df5<-df5[which(df5$RNP1!=""),]
df5<-df5[which(df5$Region!=""),]

### cross join with date ###

df6<-merge(df5,df3,all=TRUE)
colnames(df6)[3]<-"C1_Date"
df6$C1_Date<-as.Date(df6$C1_Date,format="%m/%d/%Y")

## Create a key of RNP1+Date ##
colnames(df6)
df6$RNP1_1 <- gsub(pattern = "[^A-Za-z0-9]+", x= df6$RNP1, "")
df6$C1_Date_1 <- gsub(pattern = "[^A-Za-z0-9]+", x= df6$C1_Date, "")
df6$key<-paste0(df6$Region,df6$RNP1_1,df6$C1_Date_1)

df2$RNP1_1 <- gsub(pattern = "[^A-Za-z0-9]+", x= df2$RNP1, "")
df2$C1_Date_1 <- gsub(pattern = "[^A-Za-z0-9]+", x= df2$C1_Date, "")
df2$key<-paste0(df2$Region,df2$RNP1_1,df2$C1_Date_1)

## drop blanks from RNP1 ##
df2<-df2[which(df2$RNP1!=""),]

## remove duplicated ##
colnames(df1)
df2<-df2 %>% distinct(Region,C1_Date_1,RNP1_1, .keep_all = TRUE)
sum(df2$Inv_count)#766775

## join to update invoice ##
df7 = merge(df6, df2[,c("key","Inv_count")], by=c("key"),all.x=TRUE)

# df7 <- sqldf("SELECT df6.*,df1.Inv_count 
#               FROM df6
#               LEFT JOIN df1 USING(key)")

## qc ##

sum(df$Quantity)## 1284945
sum(df2$Inv_count)##1207290
sum(df2[!is.na(df2$Inv_count),]$Inv_count)##1207290

## Remove Duplicates ##
df8<-df7 %>% distinct(Region,RNP1_1, C1_Date_1, .keep_all = TRUE)
sum(df8[!is.na(df8$Inv_count),]$Inv_count)##1207290

count(df8[which(df8$Inv_count>=0),])#158501
count(df2[which(df2$Inv_count>=0),])#158501

## replace NA to 0 in inv_count ##
df8[is.na(df8$Inv_count),]$Inv_count<-0

c1_1<-df8
write.csv(df8,"C1_1.csv")


##### Clean the Environment first ######


###-------------------------### At c1A ###-------------------------##
## read File ##
## Main Opty iD level input file ###
# library(aws.s3)
# Sys.setenv("AWS_ACCESS_KEY_ID" = "XXXXX")
# Sys.setenv("AWS_SECRET_ACCESS_KEY" = "XXXXX")
# Sys.setenv("AWS_DEFAULT_REGION" = "XXXXX")


##Read csv from s3
#rnp<-read.csv("C:\\Users\\ankit.patel1\\Desktop\\Pipeline\\Grannular_Tonnage_Sheet.csv")
df <- Master_data
#s3read_using(FUN = read.csv, object = "xxxx.csv", bucket = "xxxx")
#View(df)

sum(df$Invoice_Quantity)##411704
sum(df$Quantity)##1263941
df$C1_Date<-as.Date(df$C1_Date,format="%m/%d/%Y")
df$C1A_Date<-as.Date(df$C1A_Date,format="%m/%d/%Y")

## Map RNP1 to the main table ##

#rnp<-read.csv("C:\\Users\\ankit.patel1\\Desktop\\Pipeline\\Granular Tonnage Sheet2.csv")

library(dplyr)
rnp$RNP1<-tolower(rnp$RNP1)
rnp<-rnp[,c('VC','RNP1')]
rnp<-rnp[which(rnp$VC!=''),]
rnp<-rnp%>%distinct(VC, RNP1, .keep_all = TRUE)

library(sqldf)
sqldf("select distinct VC,count(RNP1) from rnp group by VC having count(RNP1)>1")

df<-merge(df,rnp[,c('VC','RNP1')],by=c("VC"),all.x=TRUE)

nrow(df[is.na(df$RNP1),])##45053
sqldf("select sum(Invoice_Quantity) from df where RNP1 is null")### 10570

df1<-df[which(df$RNP1!=''),]
sqldf("select sum(Invoice_Quantity) from df1 where RNP1 is null")###NA

library(sqldf)
colnames(df1)
df2<-sqldf("select distinct Region,C1A_Date,RNP1,sum(Quantity) as Inv_count from df1 group by Region,C1A_Date,RNP1")
sum(df2$Inv_count)##1208109

## Drop records where Region is null ##
nrow(df2[which(df2$Region==""),])##210

df2<-df2[which(df2$Region!=""),]

### Write c1 aggregate date ###
#setwd("C:\\Users\\ankit.patel1\\Desktop\\Pipeline")
#write.csv(df2,"C:\\Users\\ankit.patel1\\Desktop\\Pipeline\\c1_agg_Region_July2019_1.csv")

## Read Date file ## Change the dates as per the period of analysis ##

df3<-as.data.frame(seq(from=as.Date(Start_Date),to=as.Date(End_Date),by=1))
colnames(df3)<-c('Date')
# df3<-read.csv("C:\\Users\\ankit.patel1\\Desktop\\Pipeline\\Date_Month.csv")
# df3<-read.csv("C:\\Users\\ankit.patel1\\Desktop\\Pipeline\\Date_Month_1.csv")
# df3<-read.csv("C:\\Users\\ankit.patel1\\Desktop\\Pipeline\\Date_Month_2.csv")

### take columns RNP1 and rEGION ###
#colnames(df1)[3]<-"RNP1"
df4<-df1[,c("Region","RNP1")]

library(dplyr)
df5<-df4 %>% distinct(Region, RNP1, .keep_all = TRUE)
unique(df5$RNP1)##300

## drop blanks from RNP1 ##

df5<-df5[which(df5$RNP1!=""),]
df5<-df5[which(df5$Region!=""),]

### cross join with date ###

df6<-merge(df5,df3,all=TRUE)
colnames(df6)[3]<-"C1A_Date"
df6$C1A_Date<-as.Date(df6$C1A_Date,format="%m/%d/%Y")

## Create a key of RNP1+Date ##
colnames(df6)
df6$RNP1_1 <- gsub(pattern = "[^A-Za-z0-9]+", x= df6$RNP1, "")
df6$C1A_Date_1 <- gsub(pattern = "[^A-Za-z0-9]+", x= df6$C1A_Date, "")
df6$key<-paste0(df6$Region,df6$RNP1_1,df6$C1A_Date_1)

df2$RNP1_1 <- gsub(pattern = "[^A-Za-z0-9]+", x= df2$RNP1, "")
df2$C1A_Date_1 <- gsub(pattern = "[^A-Za-z0-9]+", x= df2$C1A_Date, "")
df2$key<-paste0(df2$Region,df2$RNP1_1,df2$C1A_Date_1)

## drop blanks from RNP1 ##
df2<-df2[which(df2$RNP1!=""),]

## remove duplicated ##
colnames(df1)
df2<-df2 %>% distinct(Region,C1A_Date_1,RNP1_1, .keep_all = TRUE)
sum(df2$Inv_count)#1279165

## join to update invoice ##
df7 = merge(df6, df2[,c("key","Inv_count")], by=c("key"),all.x=TRUE)

# df7 <- sqldf("SELECT df6.*,df1.Inv_count 
#               FROM df6
#               LEFT JOIN df1 USING(key)")

## qc ##

sum(df$Quantity)##1284945
sum(df2$Inv_count)##1207290
sum(df2[!is.na(df2$Inv_count),]$Inv_count)##1207290

## Remove Duplicates ##
df8<-df7 %>% distinct(Region,RNP1_1, C1A_Date_1, .keep_all = TRUE)
sum(df8[!is.na(df8$Inv_count),]$Inv_count)##614323

count(df8[which(df8$Inv_count>=0),])#99795
count(df2[which(df2$Inv_count>=0),])#100873

## replace NA to 0 in inv_count ##
df8[is.na(df8$Inv_count),]$Inv_count<-0

C1A_1<-df8
write.csv(df8,"C1A_1.csv")


##### Clean the Environment first ######


###-------------------------### At c2 ###-------------------------##
## read File ##
## Main Opty iD level input file ###
# library(aws.s3)
# Sys.setenv("AWS_ACCESS_KEY_ID" = "XXXXX")
# Sys.setenv("AWS_SECRET_ACCESS_KEY" = "XXXXX")
# Sys.setenv("AWS_DEFAULT_REGION" = "XXXXX")

##Read csv from s3

#rnp<-RNP_text_for_cargo_and_contruck_Final_03092019_Revised_RNP_model
#rnp<-read.csv("C:\\Users\\ankit.patel1\\Desktop\\Pipeline\\Grannular_Tonnage_Sheet.csv")
df <- Master_data
#s3read_using(FUN = read.csv, object = "xxxx.csv", bucket = "xxxx")
#View(df)

sum(df$Invoice_Quantity)##418254
sum(df$Quantity)##1284945
# df$C1_Date<-as.Date(df$C1_Date,format="%m/%d/%Y")
# df$C2_Date<-as.Date(df$C2_Date,format="%m/%d/%Y")

## Map RNP1 to the main table ##

#rnp<-read.csv("C:\\Users\\ankit.patel1\\Desktop\\Pipeline\\Granular Tonnage Sheet2.csv")

library(dplyr)
rnp$RNP1<-tolower(rnp$RNP1)
rnp<-rnp[,c('VC','RNP1')]
rnp<-rnp[which(rnp$VC!=''),]
rnp<-rnp%>%distinct(VC, RNP1, .keep_all = TRUE)

library(sqldf)
sqldf("select distinct VC,count(RNP1) from rnp group by VC having count(RNP1)>1")

df<-merge(df,rnp[,c('VC','RNP1')],by=c("VC"),all.x=TRUE)

nrow(df[is.na(df$RNP1),])##45053
sqldf("select sum(Invoice_Quantity) from df where RNP1 is null")### 10691

df1<-df[which(df$RNP1!=''),]
sqldf("select sum(Invoice_Quantity) from df1 where RNP1 is null")###NA

library(sqldf)
colnames(df1)
### partial billing ###
##sqldf("select * from df1 where C3_Date=''")
df2_Par<-sqldf("select distinct Region,C2_Date,RNP1,sum(Invoice_Quantity) as Inv_count from df1 where C3_Date='' and C2_Date<>'' group by Region,C2_Date,RNP1")
sum(df2_Par$Inv_count)##2941
df2_Par$C2_Date<-as.Date(df2_Par$C2_Date,format="%m/%d/%Y")

### Overall C2 basis Quantity ###
df2_Ovr<-sqldf("select distinct Region,C2_Date,RNP1,sum(Quantity) as Ovr_count from df1 where C2_Date<>'' group by Region,C2_Date,RNP1")
sum(df2_Ovr$Ovr_count)##428645
df2_Ovr$C2_Date<-as.Date(df2_Ovr$C2_Date,format="%m/%d/%Y")

## Drop records where Region is null ##
nrow(df2_Par[which(df2_Par$Region==""),])##17
nrow(df2_Ovr[which(df2_Ovr$Region==""),])##134

### remove rows with Blankl Region ###
df2_Par<-df2_Par[which(df2_Par$Region!=""),]
df2_Ovr<-df2_Ovr[which(df2_Ovr$Region!=""),]

## Remove rows woth Blank C2 Date ##
nrow(df2_Par[which(is.na(df2_Par$C2_Date)),])##0
nrow(df2_Ovr[which(is.na(df2_Ovr$C2_Date)),])##0

### Write c1 aggregate date ###
#setwd("C:\\Users\\ankit.patel1\\Desktop\\Pipeline")
#write.csv(df2,"C:\\Users\\ankit.patel1\\Desktop\\Pipeline\\c1_agg_Region_July2019_1.csv")

## Read Date file ##
df3<-as.data.frame(seq(from=as.Date(Start_Date),to=as.Date(End_Date),by=1))
colnames(df3)<-c('Date')
# df3<-read.csv("C:\\Users\\ankit.patel1\\Desktop\\Pipeline\\Date_Month.csv")
# df3<-read.csv("C:\\Users\\ankit.patel1\\Desktop\\Pipeline\\Date_Month_1.csv")
# df3<-read.csv("C:\\Users\\ankit.patel1\\Desktop\\Pipeline\\Date_Month_2.csv")

### take columns RNP1 and rEGION ###
#colnames(df1)[3]<-"RNP1"
df4<-df1[,c("Region","RNP1")]

library(dplyr)
df5<-df4 %>% distinct(Region, RNP1, .keep_all = TRUE)
unique(df5$RNP1)##306

## drop blanks from RNP1 ##

df5<-df5[which(df5$RNP1!=""),]
df5<-df5[which(df5$Region!=""),]

### cross join with date ###

df6<-merge(df5,df3,all=TRUE)
colnames(df6)[3]<-"C2_Date"
df6$C2_Date<-as.Date(df6$C2_Date)

## Create a key of RNP1+Date ##
colnames(df6)
df6$RNP1_1 <- gsub(pattern = "[^A-Za-z0-9]+", x= df6$RNP1, "")
df6$C2_Date_1 <- gsub(pattern = "[^A-Za-z0-9]+", x= df6$C2_Date, "")
df6$key<-paste0(df6$Region,df6$RNP1_1,df6$C2_Date_1)

df2_Par$RNP1_1 <- gsub(pattern = "[^A-Za-z0-9]+", x= df2_Par$RNP1, "")
df2_Par$C2_Date_1 <- gsub(pattern = "[^A-Za-z0-9]+", x= df2_Par$C2_Date, "")
df2_Par$key<-paste0(df2_Par$Region,df2_Par$RNP1_1,df2_Par$C2_Date_1)

df2_Ovr$RNP1_1 <- gsub(pattern = "[^A-Za-z0-9]+", x= df2_Ovr$RNP1, "")
df2_Ovr$C2_Date_1 <- gsub(pattern = "[^A-Za-z0-9]+", x= df2_Ovr$C2_Date, "")
df2_Ovr$key<-paste0(df2_Ovr$Region,df2_Ovr$RNP1_1,df2_Ovr$C2_Date_1)

## drop blanks from RNP1 ##
df2_Par<-df2_Par[which(df2_Par$RNP1!=""),]
df2_Ovr<-df2_Ovr[which(df2_Ovr$RNP1!=""),]

## remove duplicated ##
colnames(df1)
df2_Par<-df2_Par %>% distinct(Region,C2_Date_1,RNP1_1, .keep_all = TRUE)
sum(df2_Par$Inv_count)#2941
df2_Ovr<-df2_Ovr %>% distinct(Region,C2_Date_1,RNP1_1, .keep_all = TRUE)
sum(df2_Ovr$Ovr_count)#428450

## join to update invoice ##
df7 = merge(df6, df2_Par[,c("key","Inv_count")], by=c("key"),all.x=TRUE)
df7_1 = merge(df6, df2_Ovr[,c("key","Ovr_count")], by=c("key"),all.x=TRUE)

# df7 <- sqldf("SELECT df6.*,df1.Inv_count 
#               FROM df6
#               LEFT JOIN df1 USING(key)")

## qc ##

sum(df$Quantity)##1284945
sum(df2_Ovr$Ovr_count)##428450
sum(df2_Par$Inv_count)##2941
###sum(df2[!is.na(df2$Inv_count),]$Inv_count)##388756

## Remove Duplicates ##
df8<-df7 %>% distinct(Region,RNP1_1, C2_Date_1, .keep_all = TRUE)
df8_1<-df7_1%>% distinct(Region,RNP1_1, C2_Date_1, .keep_all = TRUE)
sum(df8[!is.na(df8$Inv_count),]$Inv_count)##2941
sum(df8_1[!is.na(df8_1$Ovr_count),]$Ovr_count)##428447

count(df8[which(df8$Inv_count>=0),])#17727
count(df8_1[which(df8_1$Ovr_count>=0),])#73712
###count(df2[which(df2$Inv_count>=0),])#69428

## replace NA to 0 in inv_count ##
df8[is.na(df8$Inv_count),]$Inv_count<-0
df8_1[is.na(df8_1$Ovr_count),]$Ovr_count<-0

## QC ##
sum(df8$Inv_count)##2941
sum(df8_1$Ovr_count)##428447

C2_Par<-df8
C2_Ovr<-df8_1

write.csv(df8,"C2_Par.csv")
write.csv(df8,"C2_Ovr.csv")


##### Clean the Environment first ######


###-------------------------### At c3 ###-------------------------##
## read File ##
## Main Opty iD level input file ###
# library(aws.s3)
# Sys.setenv("AWS_ACCESS_KEY_ID" = "XXXXX")
# Sys.setenv("AWS_SECRET_ACCESS_KEY" = "XXXXX")
# Sys.setenv("AWS_DEFAULT_REGION" = "XXXXX")


##Read csv from s3

#rnp<-read.csv("C:\\Users\\ankit.patel1\\Desktop\\Pipeline\\Grannular_Tonnage_Sheet.csv")
#Master_data<-read.csv("C:\\Users\\ankit.patel1\\Desktop\\Pipeline\\Master_Data.csv")
df <- Master_data
#s3read_using(FUN = read.csv, object = "xxxxx.csv", bucket = "xxxxx")
#View(df)

sum(df$Invoice_Quantity)##418254
sum(df$Quantity)##1284945
df$C1_Date<-as.Date(df$C1_Date,format="%m/%d/%Y")
df$C3_Date<-as.Date(df$C3_Date,format="%m/%d/%Y")

## Map RNP1 to the main table ##

#rnp<-read.csv("C:\\Users\\ankit.patel1\\Desktop\\Pipeline\\Granular Tonnage Sheet2.csv")

library(dplyr)
rnp$RNP1<-tolower(rnp$RNP1)
rnp<-rnp[,c('VC','RNP1')]
rnp<-rnp[which(rnp$VC!=''),]
rnp<-rnp%>%distinct(VC, RNP1, .keep_all = TRUE)

library(sqldf)
sqldf("select distinct VC,count(RNP1) from rnp group by VC having count(RNP1)>1")

df<-merge(df,rnp[,c('VC','RNP1')],by=c("VC"),all.x=TRUE)

nrow(df[is.na(df$RNP1),])##45053
sqldf("select sum(Invoice_Quantity) from df where RNP1 is null")### 10570

df1<-df[which(df$RNP1!=''),]
sqldf("select sum(Invoice_Quantity) from df1 where RNP1 is null")###NA

library(sqldf)
colnames(df1)
df2<-sqldf("select distinct Region,C3_Date,RNP1,sum(Invoice_Quantity) as Inv_count from df1 group by Region,C3_Date,RNP1")
sum(df2$Inv_count)##407563

## Drop records where Region is null ##
nrow(df2[which(df2$Region==""),])## 143

df2<-df2[which(df2$Region!=""),]

### Write c1 aggregate date ###
#setwd("C:\\Users\\ankit.patel1\\Desktop\\Pipeline")
#write.csv(df2,"C:\\Users\\ankit.patel1\\Desktop\\Pipeline\\c1_agg_Region_July2019_1.csv")

## Read Date file ##
df3<-as.data.frame(seq(from=as.Date(Start_Date),to=as.Date(End_Date),by=1))
colnames(df3)<-c('Date')
# df3<-read.csv("C:\\Users\\ankit.patel1\\Desktop\\Pipeline\\Date_Month.csv")
# df3<-read.csv("C:\\Users\\ankit.patel1\\Desktop\\Pipeline\\Date_Month_1.csv")
# df3<-read.csv("C:\\Users\\ankit.patel1\\Desktop\\Pipeline\\Date_Month_2.csv")

### take columns RNP1 and rEGION ###
#colnames(df1)[3]<-"RNP1"
df4<-df1[,c("Region","RNP1")]

library(dplyr)
df5<-df4 %>% distinct(Region, RNP1, .keep_all = TRUE)
unique(df5$RNP1)##306

## drop blanks from RNP1 ##

df5<-df5[which(df5$RNP1!=""),]
df5<-df5[which(df5$Region!=""),]

### cross join with date ###

df6<-merge(df5,df3,all=TRUE)
colnames(df6)[3]<-"C3_Date"
df6$C3_Date<-as.Date(df6$C3_Date,format="%m/%d/%Y")

## Create a key of RNP1+Date ##
colnames(df6)
df6$RNP1_1 <- gsub(pattern = "[^A-Za-z0-9]+", x= df6$RNP1, "")
df6$C3_Date_1 <- gsub(pattern = "[^A-Za-z0-9]+", x= df6$C3_Date, "")
df6$key<-paste0(df6$Region,df6$RNP1_1,df6$C3_Date_1)

df2$RNP1_1 <- gsub(pattern = "[^A-Za-z0-9]+", x= df2$RNP1, "")
df2$C3_Date_1 <- gsub(pattern = "[^A-Za-z0-9]+", x= df2$C3_Date, "")
df2$key<-paste0(df2$Region,df2$RNP1_1,df2$C3_Date_1)

## drop blanks from RNP1 ##
df2<-df2[which(df2$RNP1!=""),]

## remove duplicated ##
colnames(df1)
df2<-df2 %>% distinct(Region,C3_Date_1,RNP1_1, .keep_all = TRUE)
sum(df2$Inv_count)#407382

## join to update invoice ##
df7 = merge(df6, df2[,c("key","Inv_count")], by=c("key"),all.x=TRUE)

# df7 <- sqldf("SELECT df6.*,df1.Inv_count 
#               FROM df6
#               LEFT JOIN df1 USING(key)")

## qc ##

sum(df$Quantity)##1284945
sum(df2$Inv_count)##407382
sum(df2[!is.na(df2$Inv_count),]$Inv_count)##407382

## Remove Duplicates ##
df8<-df7 %>% distinct(Region,RNP1_1, C3_Date_1, .keep_all = TRUE)
sum(df8[!is.na(df8$Inv_count),]$Inv_count)##404435

count(df8[which(df8$Inv_count>=0),])#65983
count(df2[which(df2$Inv_count>=0),])#67109

## replace NA to 0 in inv_count ##
df8[is.na(df8$Inv_count),]$Inv_count<-0

C3_1<-df8
write.csv(df8,"C3_1.csv")


##### Clean the Environment first ######


####-----------------------Collating all the C1,C1A,C2 & C3 files-----------------------------####

#### Read Raw file ###
# library(aws.s3)
# Sys.setenv("AWS_ACCESS_KEY_ID" = "XXXXX")
# Sys.setenv("AWS_SECRET_ACCESS_KEY" = "XXXXX")
# Sys.setenv("AWS_DEFAULT_REGION" = "XXXXX")


##Read csv from s3


#rnp<-read.csv("C:\\Users\\ankit.patel1\\Desktop\\Pipeline\\Grannular_Tonnage_Sheet.csv")
##unique(rnp$RNP1)
#Master_data<-read.csv("C:\\Users\\ankit.patel1\\Desktop\\Pipeline\\Master_Data.csv")
df <- Master_data
#s3read_using(FUN = read.csv, object = "xxxxx.csv", bucket = "xxxxx")

df$C1_Date<-as.Date(df$C1_Date,format="%m/%d/%Y")
#rnp<-read.csv("C:\\Users\\ankit.patel1\\Desktop\\Pipeline\\Granular Tonnage Sheet2.csv")

library(dplyr)
rnp$RNP1<-tolower(rnp$RNP1)
rnp<-rnp[,c('VC','RNP1')]
rnp<-rnp[which(rnp$VC!=''),]
rnp<-rnp%>%distinct(VC, RNP1, .keep_all = TRUE)

library(sqldf)
sqldf("select distinct VC,count(RNP1) from rnp group by VC having count(RNP1)>1")

df<-merge(df,rnp[,c('VC','RNP1')],by=c("VC"),all.x=TRUE)

nrow(df[is.na(df$RNP1),])##44757
sqldf("select sum(Invoice_Quantity) from df where RNP1 is null")### 10570

df1<-df[which(df$RNP1!=''),]
sqldf("select sum(Invoice_Quantity) from df1 where RNP1 is null")###NA

### Cross join with date ### Change the date as per the period of analysis ##
df3<-as.data.frame(seq(from=as.Date(Start_Date),to=as.Date(End_Date),by=1))
colnames(df3)<-c('Date')
# df3<-read.csv("C:\\Users\\ankit.patel1\\Desktop\\Pipeline\\Date_Month_2.csv")

df2<-df1[,c("Region","RNP1")]
df2<-df2%>%distinct(Region, RNP1, .keep_all = TRUE)

## Merge Delaer Region to Date file ##
df4<-merge(df2,df3,all=TRUE)
df4$Date<-as.Date(df4$Date,format="%m/%d/%Y")

## create a key ##

df4$RNP1_1 <- gsub(pattern = "[^A-Za-z0-9]+", x= df4$RNP1, "")
df4$Date_1 <- gsub(pattern = "[^A-Za-z0-9]+", x= df4$Date, "")
df4$key<-paste0(df4$Region,df4$RNP1_1,df4$Date_1)

## Remove duplicated
df4<-df4%>%distinct(Region, RNP1_1,Date_1, .keep_all = TRUE)

## remove all records where region is blank ###

df4<-df4[which(df4$Region!=""),]

## read c1,c1a,c2,c3 file ## Read all the C1 , c1A,C2 & c3 files separately written above ###
###rm(rnp)
getwd()
c1_1<-read.csv("C1_1.csv")
c1<-c1_1#read.csv("C:\\Users\\ankit.patel1\\Desktop\\Pipeline\\MHCV_Retail_Region_3yrs_C1_1.csv")
c1A_1<-read.csv("C1A_1.csv")
c1a<-c1A_1 #read.csv("C:\\Users\\ankit.patel1\\Desktop\\Pipeline\\MHCV_Retail_Region_3yrs_C1A_1.csv")
c2_Par<-C2_Par
c2_Ovr<-C2_Ovr
c3_1<-read.csv("C3_1.csv")
c3<-c3_1#read.csv("C:\\Users\\ankit.patel1\\Desktop\\Pipeline\\MHCV_Retail_Region_3yrs_C3_1.csv")

sum(c1$Inv_count)#1207290
sum(c1a$Inv_count)#614323
sum(c2_Par$Inv_count)#2941
sum(c2_Ovr$Ovr_count)#428447
sum(c3$Inv_count)#404435

## left join with df4 to update invoice count ##
#rm(df6)
df5<-merge(df4, c1[,c("key","Inv_count")], by=c("key"),all.x=TRUE)
#unique(df5$Inv_count)
colnames(df5)[7]<-"C1"
df6<-merge(df5, c1a[,c("key","Inv_count")], by=c("key"),all.x=TRUE)
#unique(df6$Inv_count)
colnames(df6)[8]<-"C1A"
df7<-merge(df6, c2_Par[,c("key","Inv_count")], by=c("key"),all.x=TRUE)
#unique(df7$Inv_count)
colnames(df7)[9]<-"C2_Par"
df8<-merge(df7, c2_Ovr[,c("key","Ovr_count")], by=c("key"),all.x=TRUE)
#unique(df8$Inv_count)
colnames(df8)[10]<-"C2_Ovr"
df9<-merge(df8, c3[,c("key","Inv_count")], by=c("key"),all.x=TRUE)
#unique(df9$Inv_count)
colnames(df9)[11]<-"C3"

## subset for Region ##

region <- c("North","South","East","West")
df9$RNP1<-as.character(df9$RNP1)
df9$Region<-as.character(df9$Region)

##QC##
unique(df9$Region)
df9[is.na(df9$Region),]

df10 <- df9[df9$Region %in% region, ]
unique(df10$Region)

## QC ##
sum(df9$C1)##1309799
sum(df9$C1A)##683029
sum(df9$C2_Par)##3284
sum(df9$C2_Ovr)##458253
sum(df9$C3)##431785

## Write ##
write.csv(df9,"MHCV_pipeline_Apr2016_23Mar2020.csv")

##### Clean the Environment first ######


########---------- D A T A P R E P A R A T I O N -----------#########

#---------------------------------------------------------------

#---------------------------------------------------------------
library(dplyr)
library(sqldf)
library(fmsb)

# Import files
getwd()
#setwd("C:\\Users\\ankit.patel1\\Desktop\\Pipeline")
df <-read.csv("MHCV_pipeline_Apr2016_23Mar2020.csv")

#---------------------------------------------------------------##

## sET A kEY for RNP1 and Date ##
colnames(df)
df$RNP1_1 <- gsub(pattern = "[^A-Za-z0-9]+", x= df$RNP1, "")
colnames(df)[5]<-"Date_as_of"
df$Date_as_of<-as.Date(df$Date_as_of)

## Drop duplicated ##
sqldf("select distinct count(*) from df")##1369600
df1<-df%>%distinct(Region, RNP1_1,Date_1, .keep_all = TRUE)

library(lubridate)
df1$Year<-year(df1$Date_as_of)
df1$Month<-month(df1$Date_as_of)
df1$month_year<-paste0(df1$Year,df1$Month,sep="")

## Create Key ##
df1$key1<-paste0(df1$Region,df1$RNP1_1,df1$month_year)

##Create a variable for C2=C2_Ovr-C2_Par
df1$C2<-df1$C2_Ovr-df1$C2_Par

##Create Cumulative variables for C1,C1A,C2 & C3 #######
colnames(df1)
df2<-df1%>%select(key,Region,RNP1,Date_as_of,RNP1_1,Date_1,C1,C1A,C2,C3,Year,Month,key1)%>%group_by(key1)%>%mutate(C1_cum=cumsum(C1))
df3<-df2%>%select(key,Region,RNP1,Date_as_of,RNP1_1,Date_1,C1,C1A,C2,C3,Year,Month,key1,C1_cum)%>%group_by(key1)%>%mutate(C1A_cum=cumsum(C1A))
df4<-df3%>%select(key,Region,RNP1,Date_as_of,RNP1_1,Date_1,C1,C1A,C2,C3,Year,Month,key1,C1_cum,C1A_cum)%>%group_by(key1)%>%mutate(C2_cum=cumsum(C2))
df5<-df4%>%select(key,Region,RNP1,Date_as_of,RNP1_1,Date_1,C1,C1A,C2,C3,Year,Month,key1,C1_cum,C1A_cum,C2_cum)%>%group_by(key1)%>%mutate(C3_cum=cumsum(C3))

## Subset Data for South Region ###
unique(df5$Region)
south<-df5[which(df5$Region=="South"),]
east<-df5[which(df5$Region=="East"),]
north<-df5[which(df5$Region=="North"),]
west<-df5[which(df5$Region=="West"),]

south$Region<-factor(south$Region)
east$Region<-factor(east$Region)
north$Region<-factor(north$Region)
west$Region<-factor(west$Region)

# 1. Create Lag Variables
#South 
library(data.table)
south_1<-data.table(south)
south_1[, C1_lag:=c(NA, C1_cum[-.N]), by=key1]
south_1[, C1A_lag:=c(NA, C1A_cum[-.N]), by=key1]
south_1[, C2_lag:=c(NA, C2_cum[-.N]), by=key1]
south_1[, C3_lag:=c(NA, C3_cum[-.N]), by=key1]
#North
north_1<-data.table(north)
north_1[, C1_lag:=c(NA, C1_cum[-.N]), by=key1]
north_1[, C1A_lag:=c(NA, C1A_cum[-.N]), by=key1]
north_1[, C2_lag:=c(NA, C2_cum[-.N]), by=key1]
north_1[, C3_lag:=c(NA, C3_cum[-.N]), by=key1]
#east
east_1<-data.table(east)
east_1[, C1_lag:=c(NA, C1_cum[-.N]), by=key1]
east_1[, C1A_lag:=c(NA, C1A_cum[-.N]), by=key1]
east_1[, C2_lag:=c(NA, C2_cum[-.N]), by=key1]
east_1[, C3_lag:=c(NA, C3_cum[-.N]), by=key1]
#west
west_1<-data.table(west)
west_1[, C1_lag:=c(NA, C1_cum[-.N]), by=key1]
west_1[, C1A_lag:=c(NA, C1A_cum[-.N]), by=key1]
west_1[, C2_lag:=c(NA, C2_cum[-.N]), by=key1]
west_1[, C3_lag:=c(NA, C3_cum[-.N]), by=key1]

## Convert to data frame ##

south_2<-as.data.frame(south)
north_2<-as.data.frame(north)
east_2<-as.data.frame(east)
west_2<-as.data.frame(west)

## replace NA with 0  ##

# north_2$C1_lag<-ifelse(is.na(north_2$C1_lag),0,north_2$C1_lag)
# north_2$C1A_lag<-ifelse(is.na(north_2$C1A_lag),0,north_2$C1A_lag)
# north_2$C2_lag<-ifelse(is.na(north_2$C2_lag),0,north_2$C2_lag)
# north_2$C3_lag<-ifelse(is.na(north_2$C3_lag),0,north_2$C3_lag)
# 
# south_2$C1_lag<-ifelse(is.na(south_2$C1_lag),0,south_2$C1_lag)
# south_2$C1A_lag<-ifelse(is.na(south_2$C1A_lag),0,south_2$C1A_lag)
# south_2$C2_lag<-ifelse(is.na(south_2$C2_lag),0,south_2$C2_lag)
# south_2$C3_lag<-ifelse(is.na(south_2$C3_lag),0,south_2$C3_lag)
# 
# east_2$C1_lag<-ifelse(is.na(east_2$C1_lag),0,east_2$C1_lag)
# east_2$C1A_lag<-ifelse(is.na(east_2$C1A_lag),0,east_2$C1A_lag)
# east_2$C2_lag<-ifelse(is.na(east_2$C2_lag),0,east_2$C2_lag)
# east_2$C3_lag<-ifelse(is.na(east_2$C3_lag),0,east_2$C3_lag)
# 
# west_2$C1_lag<-ifelse(is.na(west_2$C1_lag),0,west_2$C1_lag)
# west_2$C1A_lag<-ifelse(is.na(west_2$C1A_lag),0,west_2$C1A_lag)
# west_2$C2_lag<-ifelse(is.na(west_2$C2_lag),0,west_2$C2_lag)
# west_2$C3_lag<-ifelse(is.na(west_2$C3_lag),0,west_2$C3_lag)


# 4. Create Variables - Conversion ratios

# df4 <- df4 %>% 
#   mutate(C0toC1 = if_else(C0 == 0, 0, (C1/C0)))
# df4 <- df4 %>% 
#   mutate(C0toC2 = if_else(C0 == 0, 0, (C2/C0)))
# df4 <- df4 %>% 
#   mutate(C0toC3 = if_else(C0 == 0, 0, (C3/C0)))
south_2 <- south_2 %>% 
  mutate(C1toC2 = if_else(C1_cum == 0, 0, (C2_cum/C1_cum)))
south_2 <- south_2 %>% 
  mutate(C1toC3 = if_else(C1_cum == 0, 0, (C3_cum/C1_cum)))
south_2 <- south_2 %>% 
  mutate(C2toC3 = if_else(C2_cum == 0, 0, (C3_cum/C2_cum)))

north_2 <- north_2 %>% 
  mutate(C1toC2 = if_else(C1_cum == 0, 0, (C2_cum/C1_cum)))
north_2 <- north_2 %>% 
  mutate(C1toC3 = if_else(C1_cum == 0, 0, (C3_cum/C1_cum)))
north_2 <- north_2 %>% 
  mutate(C2toC3 = if_else(C2_cum == 0, 0, (C3_cum/C2_cum)))

east_2 <- east_2 %>% 
  mutate(C1toC2 = if_else(C1_cum == 0, 0, (C2_cum/C1_cum)))
east_2 <- east_2 %>% 
  mutate(C1toC3 = if_else(C1_cum == 0, 0, (C3_cum/C1_cum)))
east_2 <- east_2 %>% 
  mutate(C2toC3 = if_else(C2_cum == 0, 0, (C3_cum/C2_cum)))

west_2 <- west_2 %>% 
  mutate(C1toC2 = if_else(C1_cum == 0, 0, (C2_cum/C1_cum)))
west_2 <- west_2 %>% 
  mutate(C1toC3 = if_else(C1_cum == 0, 0, (C3_cum/C1_cum)))
west_2 <- west_2 %>% 
  mutate(C2toC3 = if_else(C2_cum == 0, 0, (C3_cum/C2_cum)))

# 5. Create Variables - Advances

# df4 <- df4 %>% 
#   mutate(C0adv = if_else(C0_lag == 0, 0, ((C0-C0_lag)/C0_lag)))
# south_2 <- south_2 %>% 
#   mutate(C1adv = if_else(C1_lag == 0, 0, ((C1_cum-C1_lag)/C1_lag)))
# south_2 <- south_2 %>% 
#   mutate(C2adv = if_else(C2_lag == 0, 0, ((C2_cum-C2_lag)/C2_lag)))
# south_2 <- south_2 %>% 
#   mutate(C3adv = if_else(C3_lag == 0, 0, ((C3_cum-C3_lag)/C3_lag)))
# 
# north_2 <- north_2 %>% 
#   mutate(C1adv = if_else(C1_lag == 0, 0, ((C1_cum-C1_lag)/C1_lag)))
# north_2 <- north_2 %>% 
#   mutate(C2adv = if_else(C2_lag == 0, 0, ((C2_cum-C2_lag)/C2_lag)))
# north_2 <- north_2 %>% 
#   mutate(C3adv = if_else(C3_lag == 0, 0, ((C3_cum-C3_lag)/C3_lag)))
# 
# east_2 <- east_2 %>% 
#   mutate(C1adv = if_else(C1_lag == 0, 0, ((C1_cum-C1_lag)/C1_lag)))
# east_2 <- east_2 %>% 
#   mutate(C2adv = if_else(C2_lag == 0, 0, ((C2_cum-C2_lag)/C2_lag)))
# east_2 <- east_2 %>% 
#   mutate(C3adv = if_else(C3_lag == 0, 0, ((C3_cum-C3_lag)/C3_lag)))
# 
# west_2 <- west_2 %>% 
#   mutate(C1adv = if_else(C1_lag == 0, 0, ((C1_cum-C1_lag)/C1_lag)))
# west_2 <- west_2 %>% 
#   mutate(C2adv = if_else(C2_lag == 0, 0, ((C2_cum-C2_lag)/C2_lag)))
# west_2 <- west_2 %>% 
#   mutate(C3adv = if_else(C3_lag == 0, 0, ((C3_cum-C3_lag)/C3_lag)))

# Create Variables - C3_total_this_month as max(c3) in that month

south_2$C3_total_this_month <- with(south_2, ave(C3_cum,key1, FUN=max) )
north_2$C3_total_this_month <- with(north_2, ave(C3_cum,key1, FUN=max) )
east_2$C3_total_this_month <- with(east_2, ave(C3_cum,key1, FUN=max) )
west_2$C3_total_this_month <- with(west_2, ave(C3_cum,key1, FUN=max) )


## Create variable of C3 month end by C1 cum till specific date ##

south_2$C3_total_C1_cum<-round(ifelse(south_2$C1_cum>0,south_2$C3_total_this_month/south_2$C1_cum,0),2)
north_2$C3_total_C1_cum<-round(ifelse(north_2$C1_cum>0,north_2$C3_total_this_month/north_2$C1_cum,0),2)
east_2$C3_total_C1_cum<-round(ifelse(east_2$C1_cum>0,east_2$C3_total_this_month/east_2$C1_cum,0),2)
west_2$C3_total_C1_cum<-round(ifelse(west_2$C1_cum>0,west_2$C3_total_this_month/west_2$C1_cum,0),2)

## Take Unique Region RNP + Month +C3_total_this_month ##
## South
south_2$Month_minus1<-month(rollback(south_2$Date_as_of))
south_2$Month_minus2<-month(rollback(rollback(south_2$Date_as_of)))
south_2$Month_minus3<-month(rollback(rollback(rollback(south_2$Date_as_of))))

south_2$Year_1<-year(rollback(south_2$Date_as_of))
south_2$Year_2<-year(rollback(rollback(south_2$Date_as_of)))
south_2$Year_3<-year(rollback(rollback(rollback(south_2$Date_as_of))))

south_2$day<-day(south_2$Date_as_of)

C3<-sqldf("select distinct RNP1_1,Month,Year,C3_total_this_month from south_2")

C3_C1<-sqldf("select distinct RNP1_1,Month,Year,day,C3_total_C1_cum from south_2")

### Add C3 total Last month columns ###

south_3<-sqldf("select a.*,b.C3_total_this_month as C3_last_month from south_2 a left join C3 b on a.RNP1_1=b.RNP1_1 and a.Month_minus1=b.Month and a.Year_1=b.Year")
south_4<-sqldf("select a.*,b.C3_total_this_month as C3_last_month_1 from south_3 a left join C3 b on a.RNP1_1=b.RNP1_1 and a.Month_minus2=b.Month and a.Year_2=b.Year")
south_5<-sqldf("select a.*,b.C3_total_this_month as C3_last_month_2 from south_4 a left join C3 b on a.RNP1_1=b.RNP1_1 and a.Month_minus3=b.Month and a.Year_3=b.Year")

south_6<-sqldf("select a.*,b.C3_total_C1_cum as Conversion_Ration_last_month_1 from south_5 a left join C3_C1 b on a.RNP1_1=b.RNP1_1 and a.Month_minus1=b.Month and a.Year_1=b.Year and a.day=b.day")

## North
north_2$Month_minus1<-month(rollback(north_2$Date_as_of))
north_2$Month_minus2<-month(rollback(rollback(north_2$Date_as_of)))
north_2$Month_minus3<-month(rollback(rollback(rollback(north_2$Date_as_of))))

north_2$Year_1<-year(rollback(north_2$Date_as_of))
north_2$Year_2<-year(rollback(rollback(north_2$Date_as_of)))
north_2$Year_3<-year(rollback(rollback(rollback(north_2$Date_as_of))))

north_2$day<-day(north_2$Date_as_of)

C3<-sqldf("select distinct RNP1_1,Month,Year,C3_total_this_month from north_2")

C3_C1<-sqldf("select distinct RNP1_1,Month,Year,day,C3_total_C1_cum from north_2")

### Add C3 total Last month columns ###

north_3<-sqldf("select a.*,b.C3_total_this_month as C3_last_month from north_2 a left join C3 b on a.RNP1_1=b.RNP1_1 and a.Month_minus1=b.Month and a.Year_1=b.Year")
north_4<-sqldf("select a.*,b.C3_total_this_month as C3_last_month_1 from north_3 a left join C3 b on a.RNP1_1=b.RNP1_1 and a.Month_minus2=b.Month and a.Year_2=b.Year")
north_5<-sqldf("select a.*,b.C3_total_this_month as C3_last_month_2 from north_4 a left join C3 b on a.RNP1_1=b.RNP1_1 and a.Month_minus3=b.Month and a.Year_3=b.Year")

north_6<-sqldf("select a.*,b.C3_total_C1_cum as Conversion_Ration_last_month_1 from north_5 a left join C3_C1 b on a.RNP1_1=b.RNP1_1 and a.Month_minus1=b.Month and a.Year_1=b.Year and a.day=b.day")

## East
east_2$Month_minus1<-month(rollback(east_2$Date_as_of))
east_2$Month_minus2<-month(rollback(rollback(east_2$Date_as_of)))
east_2$Month_minus3<-month(rollback(rollback(rollback(east_2$Date_as_of))))

east_2$Year_1<-year(rollback(east_2$Date_as_of))
east_2$Year_2<-year(rollback(rollback(east_2$Date_as_of)))
east_2$Year_3<-year(rollback(rollback(rollback(east_2$Date_as_of))))

east_2$day<-day(east_2$Date_as_of)

C3<-sqldf("select distinct RNP1_1,Month,Year,C3_total_this_month from east_2")

C3_C1<-sqldf("select distinct RNP1_1,Month,Year,day,C3_total_C1_cum from east_2")

### Add C3 total Last month columns ###

east_3<-sqldf("select a.*,b.C3_total_this_month as C3_last_month from east_2 a left join C3 b on a.RNP1_1=b.RNP1_1 and a.Month_minus1=b.Month and a.Year_1=b.Year")
east_4<-sqldf("select a.*,b.C3_total_this_month as C3_last_month_1 from east_3 a left join C3 b on a.RNP1_1=b.RNP1_1 and a.Month_minus2=b.Month and a.Year_2=b.Year")
east_5<-sqldf("select a.*,b.C3_total_this_month as C3_last_month_2 from east_4 a left join C3 b on a.RNP1_1=b.RNP1_1 and a.Month_minus3=b.Month and a.Year_3=b.Year")

east_6<-sqldf("select a.*,b.C3_total_C1_cum as Conversion_Ration_last_month_1 from east_5 a left join C3_C1 b on a.RNP1_1=b.RNP1_1 and a.Month_minus1=b.Month and a.Year_1=b.Year and a.day=b.day")

## West
west_2$Month_minus1<-month(rollback(west_2$Date_as_of))
west_2$Month_minus2<-month(rollback(rollback(west_2$Date_as_of)))
west_2$Month_minus3<-month(rollback(rollback(rollback(west_2$Date_as_of))))

west_2$Year_1<-year(rollback(west_2$Date_as_of))
west_2$Year_2<-year(rollback(rollback(west_2$Date_as_of)))
west_2$Year_3<-year(rollback(rollback(rollback(west_2$Date_as_of))))

west_2$day<-day(west_2$Date_as_of)

C3<-sqldf("select distinct RNP1_1,Month,Year,C3_total_this_month from west_2")

C3_C1<-sqldf("select distinct RNP1_1,Month,Year,day,C3_total_C1_cum from west_2")

### Add C3 total Last month columns ###

west_3<-sqldf("select a.*,b.C3_total_this_month as C3_last_month from west_2 a left join C3 b on a.RNP1_1=b.RNP1_1 and a.Month_minus1=b.Month and a.Year_1=b.Year")
west_4<-sqldf("select a.*,b.C3_total_this_month as C3_last_month_1 from west_3 a left join C3 b on a.RNP1_1=b.RNP1_1 and a.Month_minus2=b.Month and a.Year_2=b.Year")
west_5<-sqldf("select a.*,b.C3_total_this_month as C3_last_month_2 from west_4 a left join C3 b on a.RNP1_1=b.RNP1_1 and a.Month_minus3=b.Month and a.Year_3=b.Year")

west_6<-sqldf("select a.*,b.C3_total_C1_cum as Conversion_Ration_last_month_1 from west_5 a left join C3_C1 b on a.RNP1_1=b.RNP1_1 and a.Month_minus1=b.Month and a.Year_1=b.Year and a.day=b.day")

### replace NA to 0 ##
south_6[is.na(south_6$C3_last_month),]$C3_last_month<-0
south_6[is.na(south_6$C3_last_month_1),]$C3_last_month_1<-0
south_6[is.na(south_6$C3_last_month_2),]$C3_last_month_2<-0
south_6[is.na(south_6$Conversion_Ration_last_month_1),]$Conversion_Ration_last_month_1<-0

north_6[is.na(north_6$C3_last_month),]$C3_last_month<-0
north_6[is.na(north_6$C3_last_month_1),]$C3_last_month_1<-0
north_6[is.na(north_6$C3_last_month_2),]$C3_last_month_2<-0
north_6[is.na(north_6$Conversion_Ration_last_month_1),]$Conversion_Ration_last_month_1<-0

west_6[is.na(west_6$C3_last_month),]$C3_last_month<-0
west_6[is.na(west_6$C3_last_month_1),]$C3_last_month_1<-0
west_6[is.na(west_6$C3_last_month_2),]$C3_last_month_2<-0
west_6[is.na(west_6$Conversion_Ration_last_month_1),]$Conversion_Ration_last_month_1<-0

east_6[is.na(east_6$C3_last_month),]$C3_last_month<-0
east_6[is.na(east_6$C3_last_month_1),]$C3_last_month_1<-0
east_6[is.na(east_6$C3_last_month_2),]$C3_last_month_2<-0
east_6[is.na(east_6$Conversion_Ration_last_month_1),]$Conversion_Ration_last_month_1<-0


### Append all the regions in a single data frame ###

df_final<-rbind(south_6,north_6,east_6,west_6)
unique(df_final$Region)

#filtering day wise data

#day22
df_final$day=day(df_final$Date_as_of)

df_day_level=filter(as.data.frame(df_final),df_final$day==22)

write.csv(df_day_level,"Pipeline_day_level_22th.csv")

###### Start of Modelling Code from here ###########

#modelling using xgboost
df_day_level<-read.csv("Pipeline_day_level_22th.csv",header=TRUE)

MHCV_3years_<-as.data.frame(df_day_level)
MHCV_3years_$yearmonth=paste0(MHCV_3years_$Year,MHCV_3years_$Month)

#filtering out april - june 2016

mhcv_region=filter(MHCV_3years_,MHCV_3years_$yearmonth!=20166,
                   MHCV_3years_$yearmonth!=20165,MHCV_3years_$yearmonth!=20164 )

a=colnames(mhcv_region)
names<-c("Region","RNP1_1","Date_1","Month","C1_cum" ,                       
         "C1A_cum" , "C2_cum" ,                       
        "C3_cum",  "C1toC2" ,                       
          "C1toC3" , "C2toC3",                        
          "C3_total_this_month","C3_last_month"   ,              
         "C3_last_month_1"    ,            "C3_last_month_2" ,              
         "Conversion_Ration_last_month_1" ,"yearmonth" )

ds<-select(mhcv_region,names)

ds$Month=as.factor(ds$Month)
str(ds)

### Create Dummies ###

#ds$Year=as.factor(ds$Year)
#install.packages("fastDummies")
library(fastDummies)
colnames(ds)
ds1=fastDummies::dummy_cols(ds[-c(17,2,3)])
ds1_cat=apply(ds1[,c(15:30)],2,
              function(x){
                return(as.numeric(x))
              })
ds2=cbind(ds[,],ds1_cat)

#pipeline predictions for ongoing month

library(xgboost)

######################################### If already tuned move to line 1354 or else proceed here ####################################

## Change the date as per the updated data 
# ds_train1<-(filter(ds2,ds2$Date_1!="20191002",ds2$Date_1!="20191102",ds2$Date_1!="20191202"))
# ds_test1<-filter(ds2,ds2$Date_1 %in% c("20191002"))
# ds_val1<-filter(ds2,ds2$Date_1 %in% c("20191102"))
ds_train1<-filter(ds2, ds2$Date_1!="20191212",ds2$Date_1!="20191112",ds2$Date_1!="20191012")
ds_test1<-filter(ds2,ds2$Date_1 %in% c("20191112","20191012"))

# ds_train1<-(filter(ds2, ds2$Date_1!="20191112",ds2$Date_1!="20191012"))
# ds_test1<-filter(ds2,ds2$Date_1 %in% c("20191012"))

#class(ds_test1)
#summary(ds_test1)
### Split into train and test ###
colnames(ds_train1)
xtrain = as.matrix(ds_train1[,-c(1:4,12,17)])
ytrain = as.matrix(ds_train1[,c(12)])
xtest = as.matrix(ds_test1[,-c(1:4,12,17)])
ytest = as.matrix(ds_test1[,c(12)])

# xval = as.matrix(ds_val1[,-c(1:4,12,17)])
# yval = as.matrix(ds_val1[,c(12)])
# colnames(xtrain)
# colnames(xtest)

### For hyperparameter tunning create a Dmatrix ###
dtrain<-xgb.DMatrix(data=xtrain,label=ytrain)
dtest<-xgb.DMatrix(data=xtest,label=ytest)
#dval<-xgb.DMatrix(data=xval,label=yval)

##### Set the hyper parameters #####
eta=c(0.05,0.1,0.2,0.5,1)
cs=c(1/3,2/3,1)
md=c(2,4,6,10)
ss=c(0.25,0.5,0.75,1)

## Default parameters ##
standard=c(2,3,3,4)

## Default parameters from below tuning code ###
params=list(eta = eta[2], colsample_bylevel=cs[3],
            subsample = ss[4], max_depth = md[3])

## find the optimal value of nrounds with default params##
set.seed(1)
xgb2<-xgb.cv(params=params,data=dtrain,nrounds=500,early_stopping_rounds =20 ,nfold=2,metrics = list("mae"),maximize = FALSE,stratified=FALSE)
min(xgb2$evaluation_log$test_mae_mean)##3.54 with optimal rounds = 130

# ## train model with optimal rounds ##
# set.seed(1)
# xgb2<-xgb.train(params=params,data=dtrain,nrounds=130,watchlist=list(val=dtest,train=dtrain),early_stopping_rounds = 20,eval_metric=list("mae"),maximize = FALSE)
# xgb2$best_score###1.86
# 
# ## xgb importance ##
# mat <- xgb.importance (feature_names = colnames(dtrain),model = xgb2)
# xgb.plot.importance (importance_matrix = mat[1:20])

######################################### Usign Grid Search ####################################
########### Use the nrounds found from above xgb.cv and eta fixed and find the optimal paramaters ########
#create a task
colnames(ds_train1)
colnames(ds_train1) <- make.names(colnames(ds_train1),unique = T)
traintask <- makeRegrTask(data = ds_train1[,-c(1:4,17)],target = "C3_total_this_month")
colnames(ds_test1) <- make.names(colnames(ds_test1),unique = T)
testtask <- makeRegrTask(data = ds_test1[,-c(1:4,17)],target = "C3_total_this_month")

#set 2 fold cross validation rf
xgdesc <- makeResampleDesc("CV",iters=2L)

## Xgboost learner :: Set the nrounds from above xgb.cv function keeping all other default parameters
set.seed(1)
xg.lrn <- makeLearner("regr.xgboost",predict.type = "response",
                      par.vals = list(objective = "reg:linear",
                                      eval_metric = "rmse",
                                      nrounds = 130,
                                      eta=0.1))

## set tunable parameter for xgboost 
xg_ps <- makeParamSet(
  makeDiscreteParam("booster",values = c("gbtree","gblinear")),
  makeIntegerParam("max_depth",lower=3,upper=10),
  makeNumericParam("subsample", lower = 0.10, upper = 0.80),
  makeNumericParam("min_child_weight",lower=1,upper=5),
  makeNumericParam("colsample_bytree",lower = 0.2,upper = 0.8)
)

##grid search 
randcontrol <- mlr::makeTuneControlRandom(maxit=100L) #do 100 iterations

#set parallel backend (Windows)

library(parallelMap)
library(parallel)
parallelStartSocket(cpus = detectCores())

## tuning start ##
a<-Sys.time()
xgb_tune <- tuneParams(learner = xg.lrn, resampling = xgdesc, task = traintask, par.set = xg_ps, control=randcontrol)
b<-Sys.time()
b-a

## Fetch tuned Parameters
xgb_tune$x
#using hyperparameters for modeling
xg_b <- setHyperPars(xg.lrn, par.vals = xgb_tune$x)

## Train the model using the training set##
xgb<-train(xg_b,traintask)

## Summary of model ##
getFeatureImportance(object=xgb)

#make predictions
xgmodel<-predict(xgb,testtask)

## Test appends xgb
xgmodel$data$response
pred_xg = round(xgmodel$data$response,2)

## Set the prediction below 0.9 to 0 ####
pred_xg[pred_xg<0.9]<-0
prediction_xgb<-pred_xg
sum(prediction_xgb)##13747

## Add Tonnage Column ###
test<-cbind(ds_test1[,-c(18:33)],prediction_xgb)

rnp<-read.csv("Grannular_Tonnage_Sheet_MHCV_20191202.csv")
rnp$RNP1_L<-tolower(rnp$RNP1)
rnp$RNP1_1 <- gsub(pattern = "[^A-Za-z0-9]+", x= rnp$RNP1_L, "")

## Map the Tonnage ##
colnames(rnp)
rnp<-rnp[,c('Tonnage','Tonnage.Aggregate','LOB','RNP1_1','RNP1','RNP1_L')]
#rnp<-rnp[,c('Tonnage_Aggregate','Tonnage','IL_Norms_Groupings','RNP1_1','RNP1','LOB','RNP1_L')]
rnp<-rnp%>%distinct(RNP1_1, .keep_all = TRUE)
unique(rnp$RNP1_1)##288

## Create a test Data frame ##
rnp<-rnp[,c('Tonnage','Tonnage.Aggregate','LOB','RNP1_1','RNP1','RNP1_L')]
region<-as.data.frame(c('North','South','East','West'))
colnames(region)<-"Region"
## Cross join ##
rnp<-merge(rnp,region,all.x = TRUE,all.y = TRUE)
## Create key 
rnp$RNP1_1<-gsub(pattern = "[^A-Za-z0-9]",x=rnp$RNP1_L,"")

colnames(test)
colnames(rnp)
test<-sqldf("select a.*,b.Date_1,b.Month,b.C1_cum,b.C1A_cum,b.C2_cum,b.C3_cum,b.C1toC2,b.C1toC3,b.C2toC3,b.C3_total_this_month,b.C3_last_month,b.C3_last_month_1,b.C3_last_month_2,b.Conversion_Ration_last_month_1,b.yearmonth,prediction_xgb from rnp a left join test b on a.RNP1_1=b.RNP1_1 and a.Region=b.Region")

## Set Na's to 0 
test[is.na(test$prediction_xgb),]$prediction_xgb<-0
sum(test$prediction_xgb)

## Adding Top 50 RNP flag basis last month C3 sales ##

# percentiles
test<-test %>% mutate(PCT = ntile(C3_last_month_2, 100))
test$Top_50<-ifelse(test$PCT<=50,0,1)

### Write the file ###
colnames(test)
write.csv(test[,c(7:23,1,2,25,3,5)],"MHCV_Test_Buses_xgb_20191216.csv",row.names = FALSE)

### Keep the n_rounds and eta fixed and tune the other parameters ###

#------------------------------ Parameter Tuning -------------------------------#
################# Using Random search ###############
## ETA ##

set.seed(1)
conv_eta = matrix(NA,500,length(eta))
pred_eta = matrix(NA,nrow(ds_test1), length(eta))
colnames(conv_eta) = colnames(pred_eta) = eta

### loop ###

for(i in 1:length(eta)){
  params=list(eta = eta[i], colsample_bylevel=cs[standard[2]],
              subsample = ss[standard[4]], max_depth = md[standard[3]],
              min_child_weigth = 1)
  xgb=xgboost(xtrain, label = ytrain, nrounds = 500, params = params)
  conv_eta[,i] = xgb$evaluation_log$train_rmse
  pred_eta[,i] = predict(xgb, xtest)
}

library(ggplot2)
library(reshape2)

conv_eta = data.frame(iter=1:500, conv_eta)
conv_eta = melt(conv_eta, id.vars = "iter")
ggplot(data = conv_eta) + geom_line(aes(x = iter, y = value, color = variable))
## RMSE ##
RMSE_eta1 = sqrt(colMeans((ytest-pred_eta[,1])^2))##7.829319
RMSE_eta2 = sqrt(colMeans((ytest-pred_eta[,2])^2))##8.842136
RMSE_eta3 = sqrt(colMeans((ytest-pred_eta[,3])^2))##8.842402
RMSE_eta4 = sqrt(colMeans((ytest-pred_eta[,4])^2))##10.53381
RMSE_eta5 = sqrt(colMeans((ytest-pred_eta[,5])^2))##20.13503

## CS ##
set.seed(1)
conv_cs = matrix(NA,28,length(cs))
pred_cs = matrix(NA,nrow(ds_test1), length(cs))
colnames(conv_cs) = colnames(pred_cs) = cs

### loop ###
for(i in 1:length(cs)){
  params=list(eta = eta[standard[1]], colsample_bylevel=cs[i],
              subsample = ss[standard[4]], max_depth = md[standard[3]],
              eval_metrics="rmse")
  xgb=xgboost(xtrain, label = ytrain, nrounds = 28, params = params)
  conv_cs[,i] = xgb$evaluation_log$train_rmse
  pred_cs[,i] = predict(xgb, xtest)
}

library(ggplot2)
library(reshape2)

conv_cs = data.frame(iter=1:28, conv_cs)
conv_cs = melt(conv_cs, id.vars = "iter")
ggplot(data = conv_cs) + geom_line(aes(x = iter, y = value, color = variable))
## RMSE ##
RMSE_cs1 = sqrt(colMeans((ytest-pred_cs[,1])^2))##7.609103
RMSE_cs2 = sqrt(colMeans((ytest-pred_cs[,2])^2))##7.650204
RMSE_cs3 = sqrt(colMeans((ytest-pred_cs[,3])^2))##7.830303

## SS ##
set.seed(1)
conv_ss = matrix(NA,28,length(ss))
pred_ss = matrix(NA,nrow(ds_test1), length(ss))
colnames(conv_ss) = colnames(pred_ss) = ss

### loop ###
for(i in 1:length(ss)){
  params=list(eta = eta[standard[1]], colsample_bylevel=cs[standard[2]],
              subsample = ss[i], max_depth = md[standard[3]],
              eval_metrics="rmse")
  xgb=xgboost(xtrain, label = ytrain, nrounds = 28, params = params)
  conv_ss[,i] = xgb$evaluation_log$train_rmse
  pred_ss[,i] = predict(xgb, xtest)
}

library(ggplot2)
library(reshape2)

conv_ss = data.frame(iter=1:28, conv_ss)
conv_ss = melt(conv_ss, id.vars = "iter")
ggplot(data = conv_ss) + geom_line(aes(x = iter, y = value, color = variable))
## RMSE ##
RMSE_ss1 = sqrt(colMeans((ytest-pred_ss[,1])^2))##7.587943
RMSE_ss2 = sqrt(colMeans((ytest-pred_ss[,2])^2))##7.532873
RMSE_ss3 = sqrt(colMeans((ytest-pred_ss[,3])^2))##7.830303
RMSE_ss3 = sqrt(colMeans((ytest-pred_ss[,4])^2))##7.830303

## MD ##
set.seed(1)
conv_md = matrix(NA,28,length(md))
pred_md = matrix(NA,nrow(ds_test1), length(md))
colnames(conv_md) = colnames(pred_md) = md

### loop ###
for(i in 1:length(md)){
  params=list(eta = eta[standard[1]], colsample_bylevel=cs[standard[2]],
              subsample = ss[standard[4]], max_depth = md[i],
              eval_metrics="rmse")
  xgb=xgboost(xtrain, label = ytrain, nrounds = 28, params = params)
  conv_md[,i] = xgb$evaluation_log$train_rmse
  pred_md[,i] = predict(xgb, xtest)
}

library(ggplot2)
library(reshape2)

conv_md = data.frame(iter=1:28, conv_md)
conv_md = melt(conv_md, id.vars = "iter")
ggplot(data = conv_md) + geom_line(aes(x = iter, y = value, color = variable))
## RMSE ##
RMSE_md1 = sqrt(colMeans((ytest-pred_md[,1])^2))##8.047388
RMSE_md2 = sqrt(colMeans((ytest-pred_md[,2])^2))##7.686962
RMSE_md3 = sqrt(colMeans((ytest-pred_md[,3])^2))##7.830303
RMSE_md4 = sqrt(colMeans((ytest-pred_md[,4])^2))##7.599594

######################################## End of Random search #####################################

############### After getting the tuned parameters start the step here ###############

### Set the train and test data ####
ds_train1<-filter(ds2, ds2$Date_1!="20200322")
ds_test1<-filter(ds2,ds2$Date_1 %in% c("20200322"))

###Split into train and test ###
colnames(ds_train1)
colnames(ds_test1)
xtrain = as.matrix(ds_train1[,-c(1:4,12,17)])
ytrain = as.matrix(ds_train1[,c(12)])
xtest = as.matrix(ds_test1[,-c(1:4,12,17)])
ytest = as.matrix(ds_test1[,c(12)])

### Drop other variables last month ####
colnames(ds_train1)
xtrain = as.matrix(ds_train1[,-c(1:4,7:8,12,13:15,17,22:33)])
ytrain = as.matrix(ds_train1[,c(12)])
xtest = as.matrix(ds_test1[,-c(1:4,7:8,12,13:15,17,22:33)])
ytest = as.matrix(ds_test1[,c(12)])

##### List of  hyper parameters #####
eta=c(0.05,0.1,0.2,0.5,1)
cs=c(1/3,2/3,1)
md=c(2,4,6,10)
ss=c(0.25,0.5,0.75,1)

## Optimised parameters from above grid search  ###
## nrounds=130 (from above grid search)
params=list(eta = 0.1, colsample_bytree=0.4921791,
            subsample = 0.6671381, max_depth = 5)

params=list(eta = 0.007, colsample_bytree=0.4921791,
            subsample = 0.6671381, max_depth = 3)
#### Set seed ####
library(xgboost)
set.seed(1)
xgb1=xgboost(xtrain, label = ytrain, nrounds = 130, params = params)
xgb.importance(model=xgb1)

### Predict ###
## test Set
pred_xgb1 = round(predict(xgb1, xtest),2)

## Set the prediction below 0.9 t0 0 ####
pred_xgb1[pred_xgb1<0.9]<-0
prediction_test<-pred_xgb1
sum(prediction_test)##3741

# ### Set the prediction below 0.9 to 0 for Val set 
# pred_val_xgb2[pred_xgb1<0.9]<-0
# prediction_val<-pred_xgb1
# sum(prediction_val)##6453

## Add Tonnage Column ###
test<-cbind(ds_test1[,-c(18:33)],prediction_test)
#val<-cbind(ds_val1[,-c(18:33)],prediction_val)

rnp<-read.csv("Grannular_Tonnage_Sheet_MHCV_20200311.csv")
rnp$RNP1_L<-tolower(rnp$RNP1)
rnp$RNP1_1 <- gsub(pattern = "[^A-Za-z0-9]+", x= rnp$RNP1_L, "")

## Map the Tonnage ##
colnames(rnp)
rnp<-rnp[,c('Tonnage','Tonnage.Aggregate','LOB','RNP1_1','RNP1','RNP1_L')]
#rnp<-rnp[,c('Tonnage_Aggregate','Tonnage','IL_Norms_Groupings','RNP1_1','RNP1','LOB','RNP1_L')]
rnp<-rnp%>%distinct(RNP1_1, .keep_all = TRUE)
unique(rnp$RNP1_1)##291

# ## Take the unique records ###
# test<-sqldf("select a.*,b.Tonnage,b.H3,b.H2,b.Model,b.RNP1,b.LOB from test a left join rnp b on a.RNP1_1=b.RNP1_1")
# sum(test$prediction)

## Create a test Data frame ##
rnp<-rnp[,c('Tonnage','Tonnage.Aggregate','LOB','RNP1_1','RNP1','RNP1_L')]
region<-as.data.frame(c('North','South','East','West'))
colnames(region)<-"Region"
## Cross join ##
rnp<-merge(rnp,region,all.x = TRUE,all.y = TRUE)
## Create key 
rnp$RNP1_1<-gsub(pattern = "[^A-Za-z0-9]",x=rnp$RNP1_L,"")

colnames(test)
colnames(rnp)
test<-sqldf("select a.*,b.Date_1,b.Month,b.C1_cum,b.C1A_cum,b.C2_cum,b.C3_cum,b.C1toC2,b.C1toC3,b.C2toC3,b.C3_total_this_month,b.C3_last_month,b.C3_last_month_1,b.C3_last_month_2,b.Conversion_Ration_last_month_1,b.yearmonth,b.prediction_test from rnp a left join test b on a.RNP1_1=b.RNP1_1 and a.Region=b.Region")
#val<-sqldf("select a.*,b.Date_1,b.Month,b.C1_cum,b.C1A_cum,b.C2_cum,b.C3_cum,b.C1toC2,b.C1toC3,b.C2toC3,b.C3_total_this_month,b.C3_last_month,b.C3_last_month_1,b.C3_last_month_2,b.Conversion_Ration_last_month_1,b.yearmonth,b.prediction_val from rnp a left join val b on a.RNP1_1=b.RNP1_1 and a.Region=b.Region")

#test<-sqldf("select a.*,b.Date_1,b.Month,b.C1_cum,b.C1A_cum,b.C2_cum,b.C3_cum,b.C1toC2,b.C1toC3,b.C2toC3,b.C3_total_this_month,b.Conversion_Ration_last_month_1,b.yearmonth,b.prediction from rnp a left join test b on a.RNP1_1=b.RNP1_1 and a.Region=b.Region")

## Set Na's to 0 
test[is.na(test$prediction_test),]$prediction_test<-0
sum(test$prediction_test)###7905

#unique(test$RNP1)

# train[is.na(train$prediction),]$prediction<-0
# sum(train$prediction)###4676.5
# unique(train$RNP1)

## Adding Top 50 RNP flag basis last month C3 sales ##

# percentiles
test<-test %>% mutate(PCT = ntile(C3_last_month_2, 100))
test$Top_50<-ifelse(test$PCT<=50,0,1)

#val<-val %>% mutate(PCT = ntile(C3_last_month_2, 100))
#val$Top_50<-ifelse(val$PCT<=50,0,1)

### Write the file ###
colnames(test)
#write.csv(test[,c(8:25,1,27,6,2:5)],"Pipeline_predictions_5Nov.csv",row.names = FALSE)
#write.csv(test[,c(7,8,9,10,11:22,23,24,2,1,26,5,4)],"Pipeline_predictions_ExcludingC3Lastmonth_14Oct.csv",row.names = FALSE)
write.csv(test[,c(7:23,1,2,25,3,4,5)],"Pipeline_predictions_ExcludingAllSales_AllMonths_C3cum_C2cum_22Mar.csv",row.names = FALSE)
write.csv(test[,c(7:23,1,2,25,3,5)],"Pipeline_predictions_Oct_Novtest.csv",row.names = FALSE)
write.csv(val[,c(7:23,1,2,25,3,5)],"Pipeline_predictions_Oct_NovVal.csv",row.names = FALSE)

########################################################################################
                                ####RNP2 level data prediction####
########################################################################################

### Exclusive at Region ###

### RNP2 , RNP, VC level data ###
rnp<-read.csv("Grannular_Tonnage_Sheet_MHCV_20200115.csv")
# rnp$RNP1_L<-tolower(rnp$RNP1)
# rnp$RNP2_L<-tolower(rnp$RNP2)
rnp$RNP1_1 <- gsub(pattern = "[^A-Za-z0-9]+", x= rnp$RNP1, "")
rnp$RNP2_1 <- gsub(pattern = "[^A-Za-z0-9]+", x= rnp$RNP2, "")

rnp<-rnp[,c('VC','RNP1_1','RNP2_1')]
rnp<-rnp%>%distinct(VC,RNP1_1,RNP2_1, .keep_all = TRUE)

## Take VC RNP1 from last 3 month train data ##
Master_data<-read.csv("Master_Data.csv")
Master_data$C1_Date<-as.Date(Master_data$C1_Date,format="%m/%d/%Y")

## Take VC RNP1 from last 2 month train data ##
VC<-Master_data[which(Master_data$C1_Date>='2019-11-01' & Master_data$C1_Date<='2019-12-31'),]

### take group by VC , Region against C3 ###
VC1<-sqldf("select distinct VC,sum(Invoice_Quantity) as Sum_C3 from VC group by VC")

## Map RNP1 and RNP2 basis VC ##
VC2<-merge(VC1,rnp,by.x=c("VC"),by.y=c("VC"),all.x = TRUE)

##QC##
sqldf("select sum(Sum_C3) from VC2 where RNP1_1 is null")###23
sqldf("select sum(Sum_C3) from VC2")###15528

### Filter the records where RNP1 is NUll ####

VC3<-VC2[!is.na(VC2$RNP1_1),]
sum(VC3$Sum_C3)##15483

## Group by RNP1 & RNP2 ##
VC4<-sqldf("Select distinct RNP1_1,RNP2_1,sum(Sum_C3) as Sum_C3_Cum from VC3 group by RNP1_1,RNP2_1")
sum(VC4$Sum_C3_Cum)##15483

## Sum C3 over RNP1  ###
VC5<-sqldf("select distinct RNP1_1,sum(Sum_C3_Cum) as Sum_C3 from VC4 group by RNP1_1")
sum(VC5$Sum_C3)##15483

### Map cum S3 sum ##
VC6<-merge(VC4,VC5[,c(1,2)],by.x=c("RNP1_1"),all.x = TRUE)

## Calculate Proportion at RNP2 level basis ACTUALS ##
VC6$Prop_Actual<-round((VC6$Sum_C3_Cum/VC6$Sum_C3),3)
VC6[which(VC6$Sum_C3==0),]

## Write the File ##
write.csv(VC6,"RNP2_Proportion_MHCV.csv")

############################################## Inclusive at Region level break ##############################################

### RNP2 , RNP, VC level data ###
rnp<-read.csv("Grannular_Tonnage_Sheet_MHCV_20200115.csv")
# rnp$RNP1_L<-tolower(rnp$RNP1)
# rnp$RNP2_L<-tolower(rnp$RNP2)
rnp$RNP1_1 <- gsub(pattern = "[^A-Za-z0-9]+", x= rnp$RNP1, "")
rnp$RNP2_1 <- gsub(pattern = "[^A-Za-z0-9]+", x= rnp$RNP2, "")

rnp<-rnp[,c('VC','RNP1_1','RNP2_1')]
rnp<-rnp%>%distinct(VC,RNP1_1,RNP2_1, .keep_all = TRUE)

## cross join with Region ##

region<-as.data.frame(c('North','South','East','West'),colnames=c('Region'))
colnames(region)<-"Region"

rnp<-merge(rnp,region,all.x = T)

## Make key of VC and Region ##
rnp$key<-paste0(rnp$VC,rnp$Region,sep="")

## Take VC RNP1 from last 3 month train data ##
Master_data<-read.csv("Master_Data.csv")
Master_data$C1_Date<-as.Date(Master_data$C1_Date,format="%m/%d/%Y")

## Take VC RNP1 from last 2 month train data ##
VC<-Master_data[which(Master_data$C1_Date>='2019-10-01' & Master_data$C1_Date<='2019-11-30'),]

### take group by VC , Region against C3 ###
VC1<-sqldf("select distinct VC,Region,sum(Invoice_Quantity) as Sum_C3 from VC group by VC,Region")

## Remove VC where blank ##
VC1<-VC1[which(VC1$VC!=""),]

## Remove Region where blank ##
VC1<-VC1[which(VC1$Region!=""),]

## Create key of VC & region ##
VC1$key<-paste0(VC1$VC,VC1$Region,sep="")

## Map RNP1 and RNP2 basis VC ##
VC2<-merge(VC1,rnp[,c(2,3,5)],by.x=c("key"),by.y=c("key"),all.x = TRUE)

sum(VC2$Sum_C3)##13781

##QC##
sqldf("select sum(Sum_C3) from VC2")###13907

### Filter the records where RNP1 is NUll ####

VC3<-VC2[!is.na(VC2$RNP1_1),]
sum(VC3$Sum_C3)##13779

## Group by RNP1 & RNP2 & region##
VC4<-sqldf("Select distinct RNP1_1,RNP2_1,Region,sum(Sum_C3) as Sum_C3_Cum from VC3 group by RNP1_1,RNP2_1,Region")
sum(VC4$Sum_C3_Cum)##13779

## Create a key of RNP1_1 and region ##
VC4$key<-paste0(VC4$RNP1_1,VC4$Region,sep="")

## Sum C3 over RNP1  & region ###
VC5<-sqldf("select distinct RNP1_1,Region,sum(Sum_C3_Cum) as Sum_C3 from VC4 group by RNP1_1,Region")
sum(VC5$Sum_C3)##13779

## Create a key of RNP1_1 and region ##
VC5$key<-paste0(VC5$RNP1_1,VC5$Region,sep="")

## remove records where Sum_C3 is 0 ###
VC5<-VC5[which(VC5$Sum_C3!=0),]

### Map cum S3 sum ##
VC6<-merge(VC4,VC5[,c(4,3)],by.x=c("key"),by.y=c("key"),all.x = TRUE)

## Drop records where Su_C3 is na (DUPLICATE RECORDS)
VC6<-VC6[!is.na(VC6$Sum_C3),]
sum(VC6$Sum_C3_Cum)##13779

## Calculate Proportion at RNP2 level basis ACTUALS ##
VC6$Prop_Actual<-round((VC6$Sum_C3_Cum/VC6$Sum_C3),3)
VC6[which(VC6$Sum_C3==0),]

## Write the File ##
write.csv(VC6,"RNP2_Proportion_MHCV.csv")
