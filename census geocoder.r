#  	R CODE
## Census geocoding services are available to the public.  Max batch size 
## is 1,000 records in a specific format.
#     Geocodes addresses - returns Census tract, lat & long info for a street address
#
# 
#     Create a RAW.CSV file stored in C:\Temp\50States directory.  The file 
#       should solely consist of the following fields:
#         RecordNumber (comma) Street Address (comma) City (comma) State (comma) Zip Code
#       DO NOT INCLUDE HEADERS.  Data only.  Remove any commas in the Street Addresses
#         or you will get an error that there are more (or less) than five elements.
#
#     When importing geographies.txt, columns CensusState,CensusCounty,
#       Tract, CensusBlock,CountyGEOID, CensusTractGEOID, Census2010BlockGEOID
#       need to be imported as TEXT.
#
#     The output file is "Geographies.txt"
#
################################################################################

## load libraries
  if(!require(openxlsx)){install.packages("openxlsx") 
    library(openxlsx)}   ## allows load of xlsx without java heap errors
  if(!require(plyr)){install.packages("plyr")
    library(plyr)}
  if(!require(dplyr)){install.packages("dplyr")
    library(dplyr)}
  if(!require(jsonlite)){install.packages("jsonlite") 
    library(jsonlite)}
  if(!require(tidyjson)){install.packages("tidyjson")
    library(tidyjson)}
  if(!require(RCurl)){install.packages("RCurl")
    library("Rcurl")}
  if(!require(RODBC)){install.packages("RODBC")
    library("RODBC")}
  if(!require(stringr)){install.packages("stringr")
    library("stringr")}
  if(!require(httr)){install.packages("httr") 
    library(httr)}  
  if(!require(data.table)){install.packages("data.table")
    library(data.table)}
  

  
  #  back out to main directory
  setwd("/..")
  ## Create standard new working directory for this project
  if(!file.exists("./Temp/50States")){dir.create("./Temp/50States")}
  if(file.exists("./Temp/50States/Geographies.txt")){file.remove("./Temp/50States/Geographies.txt")}
  setwd("C:/Temp/50States/")

## Begin the process  
  
  ## Variables
  Link<-"https://geocoding.geo.census.gov/geocoder/geographies/addressbatch"  #url for geocoder & type to execute
  AddrFile<-"C:/Temp/50States/geocode.csv"          #output file name
  Locations<-c()                   #create final table
  st<-Sys.time() #track time - x is Start Time
  r<-c()  #numeric list of records per minute 
   
  #Input File & Structure:  Pull in the 5 columns of info needed to batch run:
  #File Number, Street Address (no suite), City, State, ZipCode.  No header
  Working<-read.csv("C:/Temp/50States/Raw.csv", header = FALSE,sep =",")
  Working$V1<-as.factor(Working$V1)
  rec_ct <- sum(!is.na(Working[,1])) #get the number of records to process

  
  ## test line - reduce record count to test script.  REM out when ready to run
  #Working <-Working[-1:-71580,]
  
  ## batch is the file to send records to the text file.  Max is 1,000 records
  batch <-rbind.data.frame(Working[1:1000,])
  
  
  #census operates in batches of 1000
  #get the number of records
 
  ct <-0  #number of records to process, begin with 0 because first batch is not processed yet
  i <- sum(is.na(batch[,1]))
  #  because batches are done in blocks of 1,000. Increase the count
  #   so that the final batch can be processed
  ##i <- 0
  batch<-na.omit(batch)
  print(paste(rec_ct," records to process with Start Time: ", st))
  
######### Phase 2 Create & Process batches of addresses
 
  while (rec_ct > 0){
  
  #output 1,000 records (batch size) to a csv file which will then be posted to the census geocoder.  
    write.table(na.omit(batch),"C:/Temp/50States/geocode.csv", row.names = FALSE, col.names = FALSE, sep = ",")

## load urls & variables.  Process the data by posting the new .csv batch

  geocoded <- POST(Link, encode="multipart", 
                             body=list(addressFile=upload_file(AddrFile), 
                                       benchmark="Public_AR_Current",
                                       vintage="Current_Current"
                             )
  )
  ## WORKED - thanks to: http://stackoverflow.com/questions/26611289/curl-post-statement-to-rcurl-or-httr
  ##    full script found at:  https://github.com/dlab-geo/RGeocoding/blob/master/scripts/draft/tiger_geocoding.R

  ## send output to a throw-away text file   
  capture.output(cat(content(geocoded)), file="C:/Temp/50States/toss.csv")
  ##  the Geocoder gets rid of duplicates, so record counts might not match.
  
  ## Because no guarantee first 5 records will have all 12 fields, force the number of columns
  #read output file in to a data frame (not sure how to do these two in one step)
  mylocs <- read.table("C:/Temp/50States/toss.csv", header=FALSE, sep = ",", col.names = paste0("V",1:12),fill = TRUE)
 
   # add leading zeroes to the fields
  mylocs$V9<-ifelse(is.na(mylocs$V9),mylocs$V9,str_pad(mylocs$V9, 2, pad = "0"))
  mylocs$V10<-ifelse(is.na(mylocs$V10),mylocs$V10,str_pad(mylocs$V10, 3, pad = "0"))
  mylocs$V11<-ifelse(is.na(mylocs$V11),mylocs$V11,str_pad(mylocs$V11, 6, pad = "0"))
  mylocs$V12<-ifelse(is.na(mylocs$V12),mylocs$V12,str_pad(mylocs$V12, 4, pad = "0"))
  mylocs$CountyGEOID<-ifelse(is.na(mylocs$V9),mylocs$V9,paste0(mylocs$V9,mylocs$V10))
  mylocs$CensusTractGEOID<-ifelse(is.na(mylocs$V9),mylocs$V9,paste0(mylocs$V9,mylocs$V10,mylocs$V11))
  mylocs$Census2010BlocksGEOID<-ifelse(is.na(mylocs$V9),mylocs$V9,paste0(mylocs$V9,mylocs$V10,mylocs$V11,mylocs$V12))
  mylocs$lon = unlist(lapply(mylocs$V6, function (x) strsplit(as.character(x), ",", fixed=TRUE)[[1]][1]))
  mylocs$lat = unlist(lapply(mylocs$V6, function (x) strsplit(as.character(x), ",", fixed=TRUE)[[1]][2]))
  
  #makesure the first colum is always a vector
  mylocs$V1<-as.factor(mylocs$V1)
  Locations <-rbind(Locations, mylocs)
  
  ##  loop reduction
  Working<-Working[-1:-1000,]
  ct<-ct+(1000-i)
  batch <-rbind.data.frame(Working[1:1000,])
  i <- sum(is.na(batch[,1]))
 
  ## calculate time until complete, create timestamp info
  nt<-Sys.time() #get new system time to create a date difference
  if(rec_ct > 1000){
  rec_ct<-rec_ct - 1000   # reduce the record count by 1000 for projected complete time
  } else ##  there are less than 1000 records, so
  {rec_ct<-0}
  
  z<-as.numeric(round(difftime(nt,st, units = "min"), digits = 2))  #numeric value of minutes between new time & startime
  r<-append(r,round(ct/z,2))  #records per minute
  mr<-round(mean(r),2)    #average number of records per minute
  min_left<-as.difftime(round(rec_ct/mr,2), units = "mins")
  
  print(paste0(ct," records processed by ", nt))
  print(paste0("Elapsed Time: ", z, " minutes"))
  
  if(i != 100){print(paste0(mr," Avg records processed per minute with ", rec_ct," records left to process.  Expected completion by ", nt+min_left))
    } else{print(paste0(mr," Avg records processed per minute."))}
  
   } 
  
  colnames(Locations)<-c("ID","Address", "MatchState", "MatchType", "MatchedAddress"
                         ,"XYCoordinates","TigerLine", "Side"
                         ,"CensusState", "CensusCounty", "Tract"
                         ,"CensusBlock","CountyGEOID"
                         ,"CensusTractGEOID"
                         ,"Census2010BlockGEOID"
                         ,"Long","Lat")                         

  
  write.table(Locations,"Geographies14.txt", sep = "\t", row.names = FALSE, col.names = TRUE, na= "", append = TRUE)

  print(paste0("Finished processing:", nt))
  
  
  
 
  ##
  ##      
  ##      https://geocoding.geo.census.gov/
  ##
  ##          https://www.census.gov/data/developers/data-sets/Geocoding-services.html
  ##
  ##    Geocoding API documentation:
  ##          https://geocoding.geo.census.gov/geocoder/Geocoding_Services_API.pdf
  ##
  ##
  ##    This one works
  ##        https://github.com/dlab-geo/RGeocoding/blob/master/scripts/draft/tiger_geocoding.R
  ##
  