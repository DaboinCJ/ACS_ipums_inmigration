# HEADER --------------------------------------------
#
# Authors: Carlos J. Daboin
# Copyright (c) Workforce of the Future Initiative, 2022
# Email:  cdaboin2@gmail.com
#
# Date: 2021-09-18
# Last Update: 2022-09-18
#
# Script Name: 202109_foreign_born_workforce_runner.R
#
# Script Description: This file calls 202109_foreign_born_workforce_function.R to process different ACS IPUMS files
#
# Notes: These functions load ACS 2019 5years by default, but @file can be changed to load/clean other files
# Outputs will be stored at data/acs_output
# This code relies on the code in code_ETL/202109_foreign_born_workforce_function.R
# Each run will load and write relatively large files and could take several minutes

source("code_ETL/202109_foreign_born_in_acs_function.R")

# 1. Clean ACS 5-year samples ---------------------------------------------

#1.1 ACS 2019 5 years --------------------------------------------------------
my_file="raw/acs_ipums/usa_00001.xml"
ipums_view(x=read_ipums_ddi(my_file))
foreign_workforce_function(file = my_file,
                           multi_year = TRUE)

# 2. Clean ACS 1-year samples ---------------------------------------------

# 2018 1-year 
ipums_view(x=read_ipums_ddi("raw/acs_ipums/usa_00050.xml"))

foreign_workforce_function(file="raw/acs_ipums/usa_00050.xml",
                           multi_year=FALSE)

# 2019 1-year 
#[DOWNLOAD 2019 SAMPLE AND USE THE FUNCTION]


# 2017 1-year 
#[DOWNLOAD 2019 SAMPLE AND USE THE FUNCTION]


# 2016 1-year 
#[DOWNLOAD 2019 SAMPLE AND USE THE FUNCTION]


# 3. Create panels from multiple ACS 1-yaer samples --------------------------------
## RUN
library(readr)
library(dplyr)
list.files("data/acs_output")

data_sets<-c("sector_acs_data_",
              "soc_minor_acs_data_",
              "cz_acs_data_",
              "cz_sector_acs_data_",
              "cz_soc_minor_acs_data_",
              "state_acs_data_",
              "state_sector_acs_data_",
              "state_soc_minor_acs_data_")

years<-as.character(2009:2019)

for(data in data_sets){
  
  samples<-list()
  
  for( t in 1:length(years) ){
    
    file_name<-paste0("data/acs_output/",data,years[t],".csv")
    
    print(paste0("loading ", file_name,"..."))
    
    samples[[t]]<-read_csv(file_name) %>% 
      mutate(year=as.numeric(years[t]))%>%
      select(year, everything())      
    
    
  }
  do.call(rbind,samples)%>% 
    write_csv(paste0("data/acs_output/",data,"panel.csv"))
  
  
}
