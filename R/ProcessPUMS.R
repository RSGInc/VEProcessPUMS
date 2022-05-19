# IMPORTS
library(data.table)
library(tools)


#
getACSPUMS <- function(STATE, YEAR='2000', output_dir = NA){
  #VARS
  state_codes <- fread('inst/extdata/state.txt')
  state_codes <- setNames(state_codes$STATE, state_codes$STUSAB)
  
  base_url = 'https://www2.census.gov/programs-surveys/acs/data/pums/'
  
  if(length(STATE) > 2 & !is.numeric(STATE)) {
    STATE <- tolower(state.abb[match(toTitleCase(STATE),state.name)])
  }
  
  # ACS PUMS to legacy Census PUMS fields
  meta = list(
    'h' = list(
      SERIALNO = list(acsname = 'SERIALNO', class ='character'),
      PUMA5 = list(acsname='PUMA', class='character'),
      HWEIGHT = list(acsname='WGTP', class='numeric'),
      UNITTYPE = list(acsname='TYPE', class='numeric'),
      PERSONS = list(acsname='NP', class='numeric'),
      BLDGSZ = list(acsname='BLD', class='character'),
      HINC = list(acsname='HINCP', class='numeric')
    ),
    'p' = list(
      SERIALNO = list(acsname = 'SERIALNO', class ='character'),
      AGE = list(acsname='AGEP', class='numeric'),
      WRKLYR = list(acsname='WKL', class='character'),
      MILITARY = list(acsname='MIL', class='numeric'),
      INCTOT = list(acsname='PINCP', class='numeric')
    )
  )
  
  
  colNames <- lapply(meta, function(x) sapply(x, function(y) y[['acsname']]))
  colclass <- lapply(meta, function(x) sapply(unname(x), function(y) {
    setNames(y[['class']], y[['acsname']])
    }))
  
  
  # Download the PUMS data to tempfile and load directly to data table
  PUMS <- lapply(c('p', 'h'), function(f) {
    url <- file.path(base_url,
                     YEAR,
                     paste0('csv_', f, tolower(STATE), '.zip'))
    
    temp <- tempfile()
    download.file(url, temp)
    
    if(Sys.info()['sysname'] == 'Windows')
      df <- fread(cmd = paste('unzip -p', temp),
                  select = names(colclass[[f]]),
                  colClasses = colclass[[f]])
    
    if(Sys.info()['sysname'] == 'Linux')
      df <- fread(cmd = paste('gunzip -cq', temp),
                  select = names(colclass[[f]]),
                  colClasses = colclass[[f]])

    # Rename ACS PUMS fields to match legacy Census PUMS fields
    setnames(df, colNames[[f]], names(colNames[[f]]))
    
    return(df)
  })
  names(PUMS) <- c('p', 'h')
  
  
  
  # SAVE OUTPUT
  if(!is.na(output_dir)) {
    if(!dir.exists(output_dir)) dir.create(output_dir)
    fwrite(PUMS[['p']], file.path(output_dir, 'pums_persons.csv'))
    fwrite(PUMS[['h']], file.path(output_dir, 'pums_households.csv'))
  } else {
    return(PUMS)
  }
}


# Downloads legacy 2000 PUMS data
getDecPUMS <- function(STATE, output_dir = NA){
  #VARS
  state_codes <- fread('state.txt')
  state_codes <- setNames(state_codes$STATE, state_codes$STUSAB)
  base_url = 'https://www2.census.gov/census_2000/datasets/PUMS/FivePercent'
  
  if(length(STATE) > 2 & !is.numeric(STATE)) {
    STATE <- state.abb[match(toTitleCase(STATE),state.name)]
  }
  STATE_NAME <- state.name[match(toupper(STATE),state.abb)]
  
  
  if(!is.numeric(STATE)) STATE_NUM <- state_codes[toupper(STATE)]
  
  
  # Download the PUMS data to tempfile and load directly to data table
  url <- file.path(base_url,
                   STATE_NAME,
                   paste0('REVISEDPUMS5_', sprintf("%02d", STATE_NUM), '.TXT'))
  
  temp <- tempfile()
  download.file(url, temp)
  
  # Read .txt to data frames
  PUMS <- read_pums(temp)
  
  # SAVE OUTPUT
  if(!is.na(output_dir)) {
    if(!dir.exists(output_dir)) dir.create(output_dir)
    fwrite(PUMS[['p']], file.path(output_dir, 'pums_persons.csv'))
    fwrite(PUMS[['h']], file.path(output_dir, 'pums_households.csv'))
  } else {
    return(PUMS)
  }
}



#=================#
#Process_2000_PUMS#
#=================#

#This script processes a Census 5% Public Use Microdata Sample Equivalency
#file (PUMEQ5) from the 2000 Census for a state to produce files that need
#to be included in the VESimHouseholds package of the VisionEval model
#system and used to estimate model parameters for the CreateHouseholds, 
#PredictWorkers, PredictIncome, and PredictHousing modules. The script
#references the file downloaded from the Census for the State of Oregon, but can
#be modified to process the data for another state. The user can download
#the data for another state from the census at the following URL:
#https://www2.census.gov/census_2000/datasets/PUMS/FivePercent/PUMS 
#After downloading the desired state file, the user will need to identify the
#path to the downloaded file in the code in SECTION A below. The user may also
#identify specific PUMAs to extract if the data for a specific metropolitan
#area or metropolitan areas are to be used rather than the data for the whole
#state. See the code in SECTION A on how to do this. Maps identifying the PUMAs
#in each state for the 2000 Census are available at the following URL:
#https://www.census.gov/geographies/reference-maps/2000/geo/2000-pumas.html


#-----------------------------------------------------------
#SECTION A - IDENTIFY THE STATE PUMS FILE AND SELECTED PUMAS
#-----------------------------------------------------------

#Identify the PUMS file for the state
#------------------------------------
#Identify the full path name to the PUMEQ5 file that data is to be processed. See
#notes above on acquiring the Census file.
# PumsFile <- "REVISEDPUMS5_41.TXT"

#Identify the PUMAs to select
#----------------------------
#If the whole state is to be selected use the following code
# GetPumas <- "ALL"
#If specific PUMAs are to be selected, list them like in the following example
#code (Oregon example)
#GetPumas <- c("41501", "41502", "41503")


#---------------------------------------------------------
#SECTION B: READ IN AND EXTRACT HOUSEHOLD AND PERSONS DATA
#---------------------------------------------------------
process_pums <- function(PumsFile, GetPumas='ALL') {
  #Read in file and split out household and person tables
  #------------------------------------------------------
  Pums_ <- readLines(PumsFile)
  RecordType_ <- 
    as.vector(sapply(Pums_, function(x) {
      substr(x, 1, 1)
      }))
  H_ <- Pums_[RecordType_ == "H"]
  P_ <- Pums_[RecordType_ == "P"]
  rm(Pums_, RecordType_, PumsFile)
  
  #Define a function to extract specified PUMS data and put in data frame
  #----------------------------------------------------------------------
  extractFromPums <- 
    function(Pums_, Fields_ls) {
      lapply(Fields_ls, function(x) {
        x$typeFun(unlist(lapply(Pums_, function(y) {
          substr(y, x$Start, x$Stop)
        })))
      })
    }
  
  #Identify the housing data to extract
  #------------------------------------
  #Define names, locations, and types for desired housing fields
  HFields_ls <-
    list(
      SERIALNO = list(Start = 2, Stop = 8, typeFun = as.character),
      PUMA5 = list(Start = 19, Stop = 23, typeFun = as.character),
      HWEIGHT = list(Start = 102, Stop = 105, typeFun = as.numeric),
      UNITTYPE = list(Start = 108, Stop = 108, typeFun = as.numeric),
      PERSONS = list(Start = 106, Stop = 107, typeFun = as.numeric),
      BLDGSZ = list(Start = 115, Stop = 116, typeFun = as.character),
      HINC = list(Start = 251, Stop = 258, typeFun = as.numeric)
    )
  
  #Extract the housing data and clean up
  #-------------------------------------
  #Extract housing data fields and put in data frame
  H_df <- data.frame(extractFromPums(H_, HFields_ls), stringsAsFactors = FALSE)
  #Extract records for desired PUMAs
  if (GetPumas[1] != "ALL") {
    H_df <- H_df[H_df$PUMA5 %in% GetPumas,]
  }
  #Clean up
  rm(H_, HFields_ls)
  
  #Identify the person data to extract
  #-----------------------------------
  #Define names, locations, and types for desired person fields
  PFields_ls <-
    list(
      SERIALNO = list(Start = 2, Stop = 8, typeFun = as.character),
      AGE = list(Start = 25, Stop = 26, typeFun = as.numeric),
      WRKLYR = list(Start = 236, Stop = 236, typeFun = as.character),
      MILITARY = list(Start = 138, Stop = 138, typeFun = as.numeric),
      INCTOT = list(Start = 297, Stop = 303, typeFun = as.numeric)
    )
  
  #Extract the person data and clean up
  #------------------------------------
  #Extract the person data fields and put in data frame
  P_df <- data.frame(extractFromPums(P_, PFields_ls), stringsAsFactors = FALSE)
  #If not getting data for entire state, limit person records to be consistent
  #with the household records for the selected PUMAs
  if (GetPumas[1] != "ALL") {
    P_df <- P_df[P_df$SERIALNO %in% unique(H_df$SERIALNO),]
  }
  rm(P_, PFields_ls)
  
  return( list('p' = P_df, 'h' = H_df) )
}


#---------------------------------------------------
#SECTION C: SAVE THE HOUSEHOLD AND PERSON DATA FILES
#---------------------------------------------------

#Save the household file
#-----------------------
# write.table(
#   H_df, 
#   file = "pums_households.csv", 
#   row.names = FALSE,
#   col.names = TRUE,
#   sep = ",")
# 
# #Save the persons file
# #---------------------
# write.table(
#   P_df, 
#   file = "pums_persons.csv", 
#   row.names = FALSE,
#   col.names = TRUE,
#   sep = ",")





