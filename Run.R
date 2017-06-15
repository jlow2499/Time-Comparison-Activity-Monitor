require(dplyr)
require(stringr)
#read trace data set
TRACE <- read.csv("//KNX3IT/AWG Management/Phone Trace Program/TRACE.csv", stringsAsFactors=FALSE)

###paste the date to the time vector 
TRACE$TIME <- paste(TRACE$TIME,TRACE$DATE)
###convert time vector to POSIXct format
TRACE$TIME <- as.POSIXct(TRACE$TIME,format="%I:%M:%S%p %m/%d/%Y")

#lag the time vector by one hour (compensate for central time)
TRACE$TIME <- TRACE$TIME - 60*60

#read df (activity history ~df month~) data set ~ Note: This will change
df <- read.csv("//knx1fs01/ED Reporting/Lowhorn Big Data/Golden Rule Data/aa.csv", stringsAsFactors=FALSE)

#paste ACT_DATE to TIME to format as POSIXct
df$TIME <- paste(df$TIME, df$ACT_DATE)

#format TIME to POSIXct
df$TIME <- as.POSIXct(df$TIME,format="%H:%M:%S %m/%d/%Y")


#format the TRACE DATE to a factor
TRACE$DATE <- as.factor(TRACE$DATE)

#set the dates variable to the unique levels of TRACE$DATE
dates <- levels(TRACE$DATE)

#subset the account activity variable 
df <- df[df$ACT_DATE %in% dates,]

#read the ARMASTEr data set
ARMASTER <- read.csv("//knx1fs01/ED Reporting/Lowhorn Big Data/Golden Rule Data/ARMASTER.csv", 
                     stringsAsFactors=FALSE)

#filter out AWG only desks
ARMASTER <- ARMASTER[ARMASTER$desk >= 800 & ARMASTER$desk <900,]

#remove hold desks
ARMASTER <- ARMASTER[ARMASTER$A.R != "",]

#we are only concerned with the person's name and employee number
ARMASTER <- ARMASTER %>% select(EMPNUM,A.R)

#split the A.R variable into two vectors of firstname and last name, assign to AR
AR <- stringr::str_split_fixed(ARMASTER$A.R,",",n=2)

#bind the two vectors in AR to ARMASTER
ARMASTER <- cbind(ARMASTER,AR)

#select only EMPNUM and lastname
ARMASTER <- ARMASTER[,c(1,3)]

#rename the column names of ARMASTER
colnames(ARMASTER) <- c("EMPNUM","AR.NAME")

#bind ARMASTER to TRACE to obtain the employee number
TRACE <- left_join(TRACE,ARMASTER,by="AR.NAME")

#remove unecessary variables from the environment
rm(AR);rm(ARMASTER);rm(dates)

#assign the unique employee numbers to a vector
EMP <- levels(as.factor(TRACE$EMPNUM))

#filter df for only the Employee numbers in EMP
df <- df[df$EMPNUM %in% EMP,]

#filter to only include LOGON and LOGOFF times
TRACE <- TRACE[TRACE$STATUS %in% c("LOGON","LOGOFF"),]

#group trace by AR and sort by TIME
TRACE <- TRACE %>%
  group_by(AR.NAME) %>%
  arrange(TIME) %>%
  ungroup()

#group trace by EMPNUM and sort by TIME
df <- df %>%
  group_by(EMPNUM) %>%
  arrange(TIME) %>%
  ungroup()

#join df to TRACE to include all of df
df <- left_join(df,TRACE,by="EMPNUM")

#create a lead vector to match the logon time with logoff time in one row in the data
df$logon <- lead(df$TIME.y,1)

#filter the lead df to only include the LOGOFF rows (logon times will be in the same row as logoff times)
df <- df[df$STATUS == "LOGOFF",]

#add a column to determine if the activity falls between the logoff and login time
df <- df %>%
  mutate(ACT_BETWEEN = ifelse(TIME.x > TIME.y & TIME.x < logon,"Yes","No")) %>%
  filter(ACT_BETWEEN == "Yes") %>%
  filter(CODE_3 != "PRM") %>%
  filter(CODE_2 != "CM") %>%
  filter(CODE_1 == "RV")

#select only the needed columns
df <- select(df,TFILE,AR.NAME,TIME.x,TIME.y,logon,CODE_1,CODE_2,CODE_3)

#rename the columns to make more sense of the data
df <- rename(df,STAR.ACT.TIME = TIME.x,logoff=TIME.y)

#write the data to the folder
write.csv(df,"//KNX3IT/AWG Management/Phone Trace Program/Dataoutput.csv")
