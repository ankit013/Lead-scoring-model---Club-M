###  lead scoring model ###
## Date ::: 2016-17
## Author :: Ankit ##
getwd()
library(RODBC)
library(dplyr)

## connection string

channel <- odbcDriverConnect(connection = "Driver={SQL Server Native Client 11.0};Server=xx.xxx.x.xx;Database=xxxx;Trusted_Connection=yes;")

# sample data

sample.data <- sqlQuery(channel, "select * from t_samplesettraining_logistmodelfinal1_anpa_160730")

names(sample.data)

# check the model
   
train.check <- sqlQuery(channel, "select * from t_april2016scoring_leads_new_anpa160820")
train.check_majjune<-sqlQuery(channel, "select * from t_mayjune2016scoring_leads_new_anpa160820")

names(train.check_majjune)

# close connection

odbcCloseAll()

#########check

names(train.check)
names(sample.data)
unique(sample.data$`Car Model`)
# glimpse(sample.data)

# response vector

my.response <- sample.data %>% select(`QMAflag`)

# matrix of predictors

my.vars <- sample.data %>% select(`Campaign Sub Type`, 
                                  `Car Make`,
                                  `Car Model`,
                                  `clean_age`,
                                  `clean_occupation`,
                                  `new_clubbed_pricing_bucket`,
                                  `clean_marital`,
                                  `new_data_source`,
                                  `no_of_columns_populated`)

# find source of redundancy
# f <- findLinearCombos(xfactor)
# 
# # soft check on car model
# s <- sort(table(my.vars$`Car Model`) / sum(table(my.vars$`Car Model`)),              decreasing = TRUE)

# create new car model variable
clean.model <- ifelse(my.vars$`Car Model`== "Swift", "A.Swift",
                      ifelse(my.vars$`Car Model` == "City", "B.City",
                             ifelse(my.vars$`Car Model` == "i20","C.i20",
                                    ifelse(my.vars$`Car Model`=="i10","D.i10","E.Others" ))))

my.vars <- my.vars %>% select(-`Car Model`)
my.vars <- mutate(my.vars, car_model = clean.model)

# clean age
clean.age <- ifelse(my.vars$clean_age == "25-28", "A. 25-28",
                    ifelse(my.vars$clean_age == "29-35", "B. 29-35",
                           ifelse(my.vars$clean_age == "36-45", "C. 36-45",
                                  ifelse(my.vars$clean_age == "46-55", "D. 46-55",
                                         ifelse(my.vars$clean_age == ">55", "E. >55",
                                                ifelse(my.vars$clean_age == "20-24","F. 20-24" ,"G. <20"))))))

my.vars <- my.vars %>% select(-clean_age)
my.vars <- mutate(my.vars, clean_age = clean.age)

# clean car make

clean.carmake <- ifelse(my.vars$`Car Make` == "Mahindra","A. Mahindra",
                        ifelse(my.vars$`Car Make`== "Chevrolet", "B. Chevrolet",
                               ifelse(my.vars$`Car Make`== "Fiat","C. Fiat",
                                      ifelse(my.vars$`Car Make`== "Ford","D. Ford",
                                             ifelse(my.vars$`Car Make`=="High End","E. High End",
                                                    ifelse(my.vars$`Car Make` == "Honda","F. Honda",
                                                           ifelse(my.vars$`Car Make`== "Hyundai","G. Hyundai",
                                                                  ifelse(my.vars$`Car Make`== "Maruti Suzuki",
                                                                         "H. Maruti Suzuki",
                                                                         ifelse(my.vars$`Car Make`== "Mitsubishi",
                                                                                "I. Mitsubishi",
                                                                                ifelse(my.vars$`Car Make` == "Nissan",
                                                                                       "J. Nissan",
                                                                                       ifelse(my.vars$`Car Make` == "Opel",
                                                                                              "K. Opel",
                                                                                              ifelse(my.vars$`Car Make` == "Renault",
                                                                                                     "L. Renault",
                                                                                                     ifelse(my.vars$`Car Make`=="Skoda",
                                                                                                            "M. Skoda",
                                                                                                            ifelse(my.vars$`Car Make`=="Ssangyong",
                                                                                                                   "N. Ssangyong",
                                                                                                                   ifelse(my.vars$`Car Make`== "Tata",
                                                                                                                          "O. Tata",
                                                                                                                          ifelse(my.vars$`Car Make`== "Toyota",
                                                                                                                                 "P. Toyota",
                                                                                                                                 "Q. Volkswagen"))))))))))))))))

my.vars <- my.vars %>% select(-`Car Make`)
my.vars <- mutate(my.vars, car_make = clean.carmake)

# clean pricing bucket

clean.price <- ifelse(my.vars$new_clubbed_pricing_bucket == "8 to 10 lac",
                      "A. 8 to 10 lac",
                      ifelse(my.vars$new_clubbed_pricing_bucket == "5 to 8 lac",
                             "B. 5 to 8 lac",
                             ifelse(my.vars$new_clubbed_pricing_bucket == "1 to 5 lac",
                                    "C. 1 to 5 lac",
                                    ifelse(my.vars$new_clubbed_pricing_bucket == "10 to 15 lac",
                                           "D. 10 to 15 lac",
                                           ifelse(my.vars$new_clubbed_pricing_bucket == "15 to 20 lac",
                                                  "E. 15 to 20 lac",
                                                  ifelse(my.vars$new_clubbed_pricing_bucket == "20 to 25 lac",
                                                         "F. 20 to 25 lac", "G. > 25 lac"))))))

my.vars <- my.vars %>% select(-new_clubbed_pricing_bucket)
my.vars <- mutate(my.vars, price_bucket = clean.price)

# store dimensions
r <- nrow(my.vars)  
c <- ncol(my.vars)

####check

names(my.vars)

factor.vars <- my.vars[,c(1:(c-5), ((c-3): c))]
# factors
xfactor <- model.matrix(my.response[,1] ~ ., data = factor.vars)[,-1]

# Design matrix
my.design <- cbind(xfactor, my.vars$no_of_columns_populated)
my.design <- data.frame(my.design)
glimpse(my.design)
dim(my.design)
#####preparing data for validation set

# matrix of predictors
names(train.check)

check.vars <- train.check[,c("Campaign Sub Type", 
                                     "Car Make",
                                     "Car Model",
                                     "clean_age",
                                     "clean_occupation",
                                     "new_clubbed_pricing_bucket",
                                     "clean_marital",
                                     "new_data_source",
                                     "no_of_columns_populated")]

check.model <- ifelse(check.vars$`Car Model`== "Swift", "A.Swift",
                      ifelse(check.vars$`Car Model` == "City", "B.City",
                             ifelse(check.vars$`Car Model` == "i20","C.i20",
                                    ifelse(check.vars$`Car Model`=="i10","D.i10","E.Others"
                                    ))))

check.vars <- check.vars %>% select(-`Car Model`)
check.vars <- mutate(check.vars, car_model = check.model)

# clean age
clean.age <- ifelse(check.vars$clean_age == "25-28", "A. 25-28",
                    ifelse(check.vars$clean_age == "29-35", "B. 29-35",
                           ifelse(check.vars$clean_age == "36-45", "C. 36-45",
                                  ifelse(check.vars$clean_age == "46-55", "D. 46-55",
                                         ifelse(check.vars$clean_age == ">55", "E. >55",
                                                ifelse(check.vars$clean_age == "20-24","F. 20-24" ,"G. <20"))))))

check.vars <- check.vars %>% select(-clean_age)
check.vars <- mutate(check.vars, clean_age = clean.age)

# clean car make

clean.carmake <- ifelse(check.vars$`Car Make` == "Mahindra","A. Mahindra",
                        ifelse(check.vars$`Car Make`== "Chevrolet", "B. Chevrolet",
                               ifelse(check.vars$`Car Make`== "Fiat","C. Fiat",
                                      ifelse(check.vars$`Car Make`== "Ford","D. Ford",
                                             ifelse(check.vars$`Car Make`=="High End","E. High End",
                                                    ifelse(check.vars$`Car Make` == "Honda","F. Honda",
                                                           ifelse(check.vars$`Car Make`== "Hyundai","G. Hyundai",
                                                                  ifelse(check.vars$`Car Make`== "Maruti Suzuki",
                                                                         "H. Maruti Suzuki",
                                                                         ifelse(check.vars$`Car Make`== "Mitsubishi",
                                                                                "I. Mitsubishi",
                                                                                ifelse(check.vars$`Car Make` == "Nissan",
                                                                                       "J. Nissan",
                                                                                       ifelse(check.vars$`Car Make` == "Opel",
                                                                                              "K. Opel",
                                                                                              ifelse(check.vars$`Car Make` == "Renault",
                                                                                                     "L. Renault",
                                                                                                     ifelse(check.vars$`Car Make`=="Skoda",
                                                                                                            "M. Skoda",
                                                                                                            ifelse(check.vars$`Car Make`=="Ssangyong",
                                                                                                                   "N. Ssangyong",
                                                                                                                   ifelse(check.vars$`Car Make`== "Tata",
                                                                                                                          "O. Tata",
                                                                                                                          ifelse(check.vars$`Car Make`== "Toyota",
                                                                                                                                 "P. Toyota",
                                                                                                                                 "Q. Volkswagen"))))))))))))))))

check.vars <- check.vars %>% select(-`Car Make`)
check.vars <- mutate(check.vars, car_make = clean.carmake)

# clean pricing bucket

clean.price <- ifelse(check.vars$new_clubbed_pricing_bucket == "8 to 10 lac",
                      "A. 8 to 10 lac",
                      ifelse(check.vars$new_clubbed_pricing_bucket == "5 to 8 lac",
                             "B. 5 to 8 lac",
                             ifelse(check.vars$new_clubbed_pricing_bucket == "1 to 5 lac",
                                    "C. 1 to 5 lac",
                                    ifelse(check.vars$new_clubbed_pricing_bucket == "10 to 15 lac",
                                           "D. 10 to 15 lac",
                                           ifelse(check.vars$new_clubbed_pricing_bucket == "15 to 20 lac",
                                                  "E. 15 to 20 lac",
                                                  ifelse(check.vars$new_clubbed_pricing_bucket == "20 to 25 lac",
                                                         "F. 20 to 25 lac", "G. > 25 lac"))))))

check.vars <- check.vars %>% select(-new_clubbed_pricing_bucket)
check.vars <- mutate(check.vars, price_bucket = clean.price)

############response from validation data

valid.response <- train.check %>% select(`QMAflag`)

r <- nrow(check.vars)  
c <- ncol(check.vars)

factor.check <- check.vars[,c((1:(c-5)), ((c-3):c))]

xfactor_valid <- model.matrix(valid.response[,1] ~ ., data = factor.check)[,-1]

# Design matrix

my.valid <- cbind(xfactor_valid, check.vars$no_of_columns_populated)
my.valid <- data.frame(my.valid)
glimpse(my.valid)
dim(my.valid)
dim(my.design)

##################mark here 5th aug 2016####################

rm(pred.scores)

pred.scores <- predict(mylogit, newdata = my.valid,
                       type = "response")


# make confusion matrix

thresh <- 0.5
pred.class <- ifelse(pred.scores >= thresh, 1, 0)
conf.mat <- table(test.response[, 1], pred.class)
conf.mat <- as.matrix(conf.mat)

accuracy <- sum(diag(conf.mat)) / sum(conf.mat)
accuracy
conf.mat

##########adding glm score to validation data

test.design$glmscore<-pred.scores
test.design<-(-test.design$glmscore)
names(test.design)

###  mutate( table_name, sample.data$leadid )  

lead_id <- train.check$'Lead ID'
location <- train.check$'Control Location'
lead_stage<-train.check$`LEAD STAGE`

copy_check.vars <- check.vars[,1:9]
copy_check.vars <- mutate(copy_check.vars, Lead_id = lead_id)
copy_check.vars <- mutate(copy_check.vars, Location = location)
copy_check.vars<- mutate(copy_check.vars, glmscore = pred.scores)
copy_check.vars<- mutate(copy_check.vars, lead_stage=lead_stage)
copy_check.vars<- mutate(copy_check.vars, responders = valid.response[,1])
names(copy_check.vars)

write.csv(copy_check.vars,"Aprilvalidation2016leadstage.csv")

# Run logistic regression

#####Stratified Sampling######

set.seed(17)
n <- nrow(my.design)
t <- 0.70 * n
t <- round(t)
train.idx <- sample(1:n, t)

# make training and test sets

train.data <- my.design[train.idx,]
train.y <- my.response[train.idx,]

test.data <- my.design[-train.idx, ]
test.y <- my.response[-train.idx, ]

########check

names(my.design)
names(my.response)

# run model on training set

mylogit <- glm(my.response$QMAflag~., data = my.design,
               family = "binomial")

# model summary

s <- summary(mylogit)
s

# fit scores

fitlogit <- mylogit$fitted.values
fitlogit

####Scoring on Training Data#######

my.design$glmscore<-fitlogit
head(my.design)
max(my.design$glmscore)
min(my.design$glmscore)

# odds ratio

odds.ratio <- exp(coef(mylogit))
o <- data.frame(round(odds.ratio, 5))
colnames(o) <- c("Odds Ratio")
o

####checks####

nrow(my.response)
nrow(my.design)

# gains curve

cuml.gains_train <- cumulative_gain(fitlogit, my.response, bins = 10, sim = 500)
plot.graph(x.info, cuml.gains_train$y.axis.info)

###  mutate( table_name, sample.data$leadid )  
lead_id <- train.check$'Lead ID'
location <- train.check$'Control Location'

copy_my.vars <- my.vars[,1:9]
copy_my.vars <- mutate(copy_my.vars, Lead_id = lead_id)
copy_my.vars <- mutate(copy_my.vars, Location = location)
copy_my.vars <- mutate(copy_my.vars, glmscore = fitlogit)
copy_my.vars <- mutate(copy_my.vars, responders = my.response[,1])


my.vars <- mutate(my.vars, lead_id)
my.vars <- mutate(my.vars, location)

names(my.vars)
names(sample.data)
head(my.vars)

my.vars<-mutate(my.vars,my.response$'QMAflag')
my.vars<-mutate(my.vars,my.design$glmscore)
head(my.vars)
getwd()
write.csv(my.vars,"Final Training 1 lac GLM scores.csv")

# Confusio matrix on training data

temp.pred <- ifelse(fitlogit >= 0.5, 1, 0)
conf.mat.train <- table(my.design, temp.pred)
conf.mat.train <- as.data.frame.matrix(conf.mat.train)
conf.mat.train <- as.matrix(conf.mat.train)

acc.train <- sum(diag(conf.mat.train)) / sum(conf.mat.train)

##############getting deciles#########
library(dplyr)

locations <- unique(copy_check.vars$Location)
l1 <- length(locations)


###########removing OTM 

copy_no_otm <- copy_check.vars %>%
               filter(`Campaign Sub Type` != 'OTM')

OTM_idx <- which(copy_check.vars$`Campaign Sub Type`=='OTM')
copy_check.vars_notOTM <- copy_check.vars[-OTM.idx,]  

# Filter and make data for each location
lhs <- paste("data", 1:l1, sep = "")
rhs <- paste("copy_no_otm %>% filter(Location == locations[", 1:l1,"])", sep = "")
eqn <- paste(lhs, rhs, sep = "<-", collapse = ";")
eval(parse(text = eqn))

# Check which location has < 10 rows
rows <- NULL
lhs2 <- paste("rows[",1:l1,"]", sep = "")
rhs2 <- paste("nrow(data", 1:l1,")", sep = "")
eqn2  <-  paste(lhs2, rhs2, sep = "<-", collapse = ";")
eval(parse(text = eqn2))

# Do deciling for each location
lhs3 <- paste("locn_score",1:15, sep = "")
rhs3 <- paste("ntile( desc(data", 1:15, "$glmscore),10)", sep = "")
eqn3 <- paste(lhs3, rhs3, sep = "<-", collapse = ";" )
eval(parse(text = eqn3))

# Mutate decile for each location data
lhs4 <- paste("data", 1:15, sep = "")
rhs4 <- paste("mutate(data", 1:15, ",decile = locn_score",1:15,")", sep = "")
eqn4 <- paste(lhs4, rhs4, sep = "<-", collapse = ";")
eval(parse(text = eqn4))

names(data1)
data1$quantile3<-quantile(data1$glmscore,.25)
data1$quantile3
rm(data1$quantile1)
data1$quantile<-quantile(data1$glmscore,.50)

data1$quantile2<-quantile(data1$glmscore,.75)
data1$quantile2
data1$quantile
data5$Location
quantile(data5$glmscore,0.90)

##########writing all data to csv

names(data1)
write.csv(data1,"data1_val.csv")
names(data2)
write.csv(data2,"data2_val.csv")
names(data3)
write.csv(data3,"data3_val.csv")
names(data4)
write.csv(data4,"data4_val.csv")
names(data5)
write.csv(data5,"data5_val.csv")
names(data6)
write.csv(data6,"data6_val.csv")
names(data7)
write.csv(data7,"data7_val.csv")
names(data8)
write.csv(data8,"data8_val.csv")
names(data9)
write.csv(data9,"data9_val.csv")
names(data10)
write.csv(data10,"data10_val.csv")
names(data11)
write.csv(data11,"data11_val.csv")
names(data12)
write.csv(data12,"data12_val.csv")
names(data13)
write.csv(data13,"data13_val.csv")
names(data14)
write.csv(data14,"data14_val.csv")
names(data15)
write.csv(data15,"data15_val.csv")
names(data2)
write.csv(data2,"data2.csv")
#########function for cumulative gain curve####

# user must provide a value to bins here and in the 
# function

bins <-10 ## number of groups to create
x.info <- seq(0, 100, by = bins)  ## X axis values
x <- x.info ## vector of X axis for cumulative gains plot
sim <- 500  ## simulations for crude estimate of AUC
seed_value <- 0.005  ## random seed

## Function will output cumulative lift values
## Arguments are vector of fitted probabilities,
## vector of observed values and number of bins to partition

make.gains <- function(score.values, observed.values,
                       bins = 10, sim = 500,
                       seed_value = 0.005){
  
  # @Arguments
  # score.values : Vector of fitted probabilities from a model
  # observed.values : Vector of observed binary values for a model
  # can be in three formats
  # 0 or 1 labels
  # "Y" or "N" labels
  # "0" or "1" as labels
  # bins : The number of groups to split score values
  # sim  : Number of simulations to calculate crude estimate of AUC
  # seed_value : Random seed to reproduce AUC Value
  
  # @Returns
  #y.axis.info : The vector of Y axis values for plotting
  # cumulative gains curve
  # Results.table  : A table containing the following columns
  # Decile number 10 being the highest and 1 the lowest
  # Responders indicating number of responders in each group
  # Non-Responders indicating number of non-responders in each group
  # lift.perc indicating percentage of responders in each group
  # cum.lift indicating number of cumulative responders
  # cum.lift.perc indicating percentage cumulative gain
  # auc_value  : Estimated AUC of the model.
  
  # Starting the checks before function evaluation
  
  # check if ggplot2, pander, Hmisc packages need to be installed
  list.of.packages <- c("ggplot2","dplyr")
  new.packages <- list.of.packages[!(list.of.packages %in%  
                                       installed.packages()[,"Package"])]      
  if(length(new.packages)) {
    install.packages(new.packages);
  }
  library(ggplot2)
  library(dplyr)
  #library(pander)
  set.seed(seed_value)
  
  ## Check if observed values is in the required format
  
  ## Stopping criteria 1
  if( any(unique(observed.values) =="Y") |
      any(unique(observed.values) =="N") ) {
    observed.values <- ifelse(observed.values == "Y", 1, 0)
  } else if
  (any(unique(observed.values) == "1") |
   any(unique(observed.values) == "0")) {
    observed.values <- ifelse(observed.values == "1", 1, 0)
  } else if
  (any(unique(observed.values) == 1) |
   any(unique(observed.values) == 0)) {
    observed.values <- observed.values
  } else {stop(print(paste("Observed values not in
                          required binary format. Please      
                           provide observed.values as a factor with
                           levels", "Y or N","or 0 and 1", 
                           "or","character 0 and character 1",
                           sep = " ")))}
  
  ## Check if predicted values are probabilities
  
  ## Stopping criteria 2
  if(any(score.values > 1) |
     any(score.values < 0) )
    stop(print(paste("Warning: Predicted scores must
                     be probabilities between 0 and 1")))
  
  ## Warn user in case predicted values are exactly 0 or 1
  if(any(score.values == 1) |
     any(score.values == 0) ) {
    print(paste("Warning: Predicted scores contain
                values which are exactly 0 or 1")) }
  
  ## Stopping criteria 3
  
  l1 <- length(score.values)
  l2 <- length(observed.values)
  if(l1 != l2) stop(
    print(paste("Observed and Score values differ in length",
                sep = "")))
  
  ## Actual start of function
  # Cut probabilities into equal groups
  # find how many of the successes lie in each group
  
  # use dplyr's ntile, mutate and count functions
  
  tiles <- ntile(score.values, bins)
  groups <- unique(tiles)
  g <- length(groups)
  
  if(g < bins) 
    stop(print(paste("There are", g, "unique score values.",
                     "Please check score values else dplyr will",
                     "forces creation of", bins, "groups",
                     sep = " ")))
  
  data <- data.frame(observed.values, score.values, tiles)
  answer <- data %>% group_by(tiles) %>% 
    summarise(total.count = n(), 
              responders = length(which(observed.values==1)),
              non.responders = length(which(observed.values==0)))
  
  r <- nrow(answer)
  # getting the top deciles at the first row
  answer <- answer[r:1 ,]
  
  # add percent lift and percent cumulative lift
  
  answer <- mutate(answer, lift.perc = responders / sum(responders))
  answer <- mutate(answer, cum.lift = cumsum(responders))
  answer <- mutate(answer, cum.lift.perc = cum.lift / 
                     sum(responders))
  colnames(answer) <- c("Decile", "Count",
                        "Responders", "Non_Responders",
                        "Percent_Responders",
                        "Cumulative_Responders",
                        "Cumulative_Percent_Responders")
  y.info <- c(0, answer$Cumulative_Percent_Responders)
  y.info <- y.info * 100
  
  # Area under the curve
  pos <- which(observed.values == 1)
  neg <- which(observed.values == 0)
  
  auc <- sum(replicate(sim, score.values[sample(pos, 1)] >
                         score.values[sample(neg, 1)]))
  auc_est <- round((auc / sim), 4) ## crude AUC estimate
  
  
  return(list(y.axis.info = y.info, 
              results.table = answer,
              auc_value = auc_est))
  }

## Graphing the lift curve
graph.gains <- function(x, y){
  pl.object <- qplot(x, y, geom = "line",
                     main = "Cumulative Gains Chart",
                     col = I("red")) + 
    geom_segment(aes(x = 0, 
                     xend = 100, y = 0, yend = 100)) 
  
  pl.object <- pl.object + 
    scale_y_continuous(name = "Lift Percent", 
                       breaks=seq(0, 100, 10)) + 
    scale_x_continuous(name = "Model Deciles", 
                       breaks=seq(0, 100, 10))  
  chart <- pl.object 
  chart
}



#######out of time validation############33

# matrix of predictors
check.vars <- train.check %>% select(`Campaign Sub Type`, 
                                     `Car Make`,
                                     `Car Model`,
                                     `clean_age`,
                                     `clean_occupation`,
                                     `new_clubbed_pricing_bucket`,
                                     `clean_marital`,
                                     `new_data_source`,
                                     `no_of_columns_populated`)


# create new car model variable
check.model <- ifelse(check.vars$`Car Model`== "Swift", "A.Swift",
                      ifelse(check.vars$`Car Model` == "City", "B.City",
                             ifelse(check.vars$`Car Model` == "i20","C.i20",
                                    ifelse(check.vars$`Car Model`=="i10","D.i10","E.Others"
                                    ))))

check.vars <- check.vars %>% select(-`Car Model`)
check.vars <- mutate(check.vars, car_model = check.model)

# clean age
clean.age <- ifelse(check.vars$clean_age == "25-28", "A. 25-28",
                    ifelse(check.vars$clean_age == "29-35", "B. 29-35",
                           ifelse(check.vars$clean_age == "36-45", "C. 36-45",
                                  ifelse(check.vars$clean_age == "46-55", "D. 46-55",
                                         ifelse(check.vars$clean_age == ">55", "E. >55",
                                                ifelse(check.vars$clean_age == "20-24","F. 20-24" ,"G. <20"))))))

check.vars <- check.vars %>% select(-clean_age)
check.vars <- mutate(check.vars, clean_age = clean.age)

# clean car make

clean.carmake <- ifelse(check.vars$`Car Make` == "Mahindra","A. Mahindra",
                        ifelse(check.vars$`Car Make`== "Chevrolet", "B. Chevrolet",
                               ifelse(check.vars$`Car Make`== "Fiat","C. Fiat",
                                      ifelse(check.vars$`Car Make`== "Ford","D. Ford",
                                             ifelse(check.vars$`Car Make`=="High End","E. High End",
                                                    ifelse(check.vars$`Car Make` == "Honda","F. Honda",
                                                           ifelse(check.vars$`Car Make`== "Hyundai","G. Hyundai",
                                                                  ifelse(check.vars$`Car Make`== "Maruti Suzuki",
                                                                         "H. Maruti Suzuki",
                                                                         ifelse(check.vars$`Car Make`== "Mitsubishi",
                                                                                "I. Mitsubishi",
                                                                                ifelse(check.vars$`Car Make` == "Nissan",
                                                                                       "J. Nissan",
                                                                                       ifelse(check.vars$`Car Make` == "Opel",
                                                                                              "K. Opel",
                                                                                              ifelse(check.vars$`Car Make` == "Renault",
                                                                                                     "L. Renault",
                                                                                                     ifelse(check.vars$`Car Make`=="Skoda",
                                                                                                            "M. Skoda",
                                                                                                            ifelse(check.vars$`Car Make`=="Ssangyong",
                                                                                                                   "N. Ssangyong",
                                                                                                                   ifelse(check.vars$`Car Make`== "Tata",
                                                                                                                          "O. Tata",
                                                                                                                          ifelse(check.vars$`Car Make`== "Toyota",
                                                                                                                                 "P. Toyota",
                                                                                                                                 "Q. Volkswagen"))))))))))))))))

check.vars <- check.vars %>% select(-`Car Make`)
check.vars <- mutate(check.vars, car_make = clean.carmake)

# clean pricing bucket

clean.price <- ifelse(check.vars$new_clubbed_pricing_bucket == "8 to 10 lac",
                      "A. 8 to 10 lac",
                      ifelse(check.vars$new_clubbed_pricing_bucket == "5 to 8 lac",
                             "B. 5 to 8 lac",
                             ifelse(check.vars$new_clubbed_pricing_bucket == "1 to 5 lac",
                                    "C. 1 to 5 lac",
                                    ifelse(check.vars$new_clubbed_pricing_bucket == "10 to 15 lac",
                                           "D. 10 to 15 lac",
                                           ifelse(check.vars$new_clubbed_pricing_bucket == "15 to 20 lac",
                                                  "E. 15 to 20 lac",
                                                  ifelse(check.vars$new_clubbed_pricing_bucket == "20 to 25 lac",
                                                         "F. 20 to 25 lac", "G. > 25 lac"))))))

check.vars <- check.vars %>% select(-new_clubbed_pricing_bucket)
check.vars <- mutate(check.vars, price_bucket = clean.price)

## non sense idx 


factor.check <- check.vars[,c((1:(c-5)), ((c-3):c))]

temp.y <- rbinom(r, 1, 0.2)
xfactor.check <- model.matrix(temp.y ~ ., data =factor.check)[,-1]
rm(temp.y)

# Design matrix
test.design <- cbind(xfactor.check, check.vars$no_of_columns_populated)
test.design <- data.frame(test.design)

############Check#########

unique(my.vars$`Campaign Sub Type`)
unique(check.vars$`Campaign Sub Type`)

#########predict scores#########
test.design<-
pred.scores <- predict(mylogit, newdata = test.design,
                       type = "response")

# Test response
test.response <- train.check %>% select(QMAflag)


# make confusion matrix
thresh <- 0.5
pred.class <- ifelse(pred.scores >= thresh, 1, 0)
conf.mat <- table(test.response[, 1], pred.class)
conf.mat <- as.matrix(conf.mat)

accuracy <- sum(diag(conf.mat)) / sum(conf.mat)
accuracy
