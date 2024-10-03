# HEADER --------------------------------------------
#
# Authors: Carlos J. Daboin
# Copyright (c) Workforce of the Future Initiative, 2023
# Email:  cdaboin2@gmail.com
#
# Date: 2023-03-15
# Last Update: 
#
# Script Name: Clean_SOC_2018_structure.R
#
# Script Description: Brief code to clean a file from the BLS with the crosswalk btw SOC detailed groups and major SOCs 2018
#
# Notes: Just reads the file in this folder called soc_structure_2018.xlsx and cleans it to have a ready to use crosswalk
#
# 0.1 Load Libraries -----------------------------
library(readxl)
library(readr)
library(dplyr)
library(janitor)
# 0.2 Load Data ----------------------------------
raw_crowsswalk<-read_xlsx("soc_structure_2018.xlsx",range = "A8:E1455") %>% 
  clean_names() %>% 
  rename(title=x5)

#1. Clean--------------------------------------------
major_soc<-raw_crowsswalk %>% 
  distinct(major_group,major_group_title=title) %>% 
  filter(!is.na(major_group))

minor_soc<-raw_crowsswalk %>% 
  distinct(minor_group,minor_group_title=title) %>% 
  filter(!is.na(minor_group))

broad_soc<-raw_crowsswalk %>% 
  distinct(broad_group,broad_group_title=title) %>% 
  filter(!is.na(broad_group))

detailed_soc<-raw_crowsswalk %>% 
  distinct(detailed_soc=detailed_occupation,detailed_soc_title=title) %>% 
  filter(!is.na(detailed_soc))

#2. Join--------------------------------------------

cleaned_crosswalk<-detailed_soc %>% 
  mutate(major_group=paste0(substr(detailed_soc,1,3),"0000"),
         minor_group=paste0(substr(detailed_soc,1,4),"000"),
         broad_group=paste0(substr(detailed_soc,1,6),"0")) %>% 
  left_join(major_soc,by="major_group") %>% 
  left_join(minor_soc, by="minor_group") %>% 
  left_join(broad_soc, by="broad_group") %>% 
  select(detailed_soc, detailed_soc_title,
         broad_group, broad_group_title,
         minor_group, minor_group_title,
         major_group, major_group_title)

write_csv(x =cleaned_crosswalk ,"soc_structure_2018_clean.csv")