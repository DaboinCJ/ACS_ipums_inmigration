# HEADER --------------------------------------------
#
# Authors: Carlos J. Daboin
# Copyright (c) Workforce of the Future Initiative, 2022
# Email:  cdaboin2@gmail.com
#
# Date: 2021-09-18
# Last Update: 2022-09-18
#
# Script Name: 202109_foreign_born_workforce_function.R
#
# Script Description: Parametrized functions that lead ACS IPUMS data sets and create tables counting population and employment by place of birth 
#
# Notes: These functions load ACS 2019 5years by default, but @file can be changed to load/clean other files
# Outputs will be stored at data/acs_output
# The function will be runned at code_ETL/202109_foreign_born_workforce_runner.R

# We're not producing standard errors for our estimates, although we could by downloading these replication weights in the PUMS batch 
# https://usa.ipums.org/usa-action/variables/REPWTP#description_section
# 0.1 Load Libraries -----------------------------
library(ipumsr)
library(tidyverse)
library(janitor)
library(haven)
library(sf)

foreign_workforce_function<-function(file="raw/acs_ipums/5-year/usa_00001.xml",
                                     multi_year=TRUE){
  

  memory.limit(size=20000)
  # 0.2 Load Data ----------------------------------
  
  
  
  #0.2 Load data--------------------
  ddi_19_15 <- read_ipums_ddi(file)
  
  data_19_15 <- read_ipums_micro(ddi_19_15) 
  
  ## PARAMETERS TO NAME FILES AND COMMENT PLOTS
  if (multi_year==TRUE){
    
    sample_description<-paste("Based in ACS samples from",paste(unique(data_19_15$MULTYEAR), collapse=", "))
    first_last_year<-paste(unique(data_19_15$MULTYEAR)[1],unique(data_19_15$MULTYEAR)[length(unique(data_19_15$MULTYEAR))], sep = "_")
    
  } else {
    sample_description<-paste("Based in ACS samples from",paste(unique(data_19_15$YEAR), collapse=", "))
    first_last_year<-paste(unique(data_19_15$YEAR)[1], sep = "_")
  }
  
  data_19_15<-data_19_15 %>%
    select(YEAR,GQ,PUMA,STATEFIP,COUNTYFIP,MET2013,
           EMPSTAT,EMPSTATD,LABFORCE, WKSWORK2,
           SEX,AGE,EDUC, EDUCD, BPL, BPLD, CITIZEN, 
           YRSUSA1, YRSUSA2, HISPAN, HISPAND,
           INCWAGE, PERWT,
           OCC,OCC2010, OCCSOC,
           IND, INDNAICS) %>%
    mutate(STATE=haven::as_factor(STATEFIP)) %>% 
    mutate_at(vars("COUNTYFIP"), ~str_pad(., width=3, side="left", pad="0")) %>%
    mutate_at(vars("STATEFIP"), ~str_pad(., width=2, side="left", pad="0")) %>%
    mutate_at(vars("COUNTYFIP"), ~paste0(STATEFIP, .))
  
  
  # Copied from parent repository on 2021-10
  # Parent repository: Crosswalk/regions/William_&_Mary/
  # downloaded from https://wmpeople.wm.edu/site/page/pmchenry/crosswalksbetweenpumasandczs
  
  puma_cz_cross_2010 <- read_dta("raw/crosswalks/William_&_Mary/puma_cz_cross_2010.dta") %>% 
    mutate(st1 = str_pad(statefip,3,"left",pad="0"),
           puma1=str_pad(puma  ,5,"left", pad="0"),
           stpuma = paste0(st1,puma1),
           stpuma = as.character(as.numeric(stpuma))) 
  
  ## read in commuter zone file (2010--note the 1990 and 2000 files are also available if needed)
  ### The data is extracted from shapefiles https://sites.psu.edu/psucz/data/
  
  cnty_to_czone_2010<-sf::read_sf("raw/czone_pennstate/czone10_shapefiles/counties10.shp") %>% 
    janitor::clean_names() %>%
    select(county_fips=fips, out10,  rep10,pop10) %>%
    mutate(county_fips=str_pad(as.character(county_fips), width=5, side="left",pad="0")) %>% 
    as.tibble()  %>% 
    select(1:4) 
  
  cnty_to_czone_1990<-sf::read_sf("raw/czone_pennstate/czone90_shapefiles/counties90.shp") %>% 
    janitor::clean_names() %>%
    select(county_fips=fips,ers90 , out90,  rep90, pop90,
           cbsa10, cbsa_name ) %>%
    mutate(county_fips=str_pad(as.character(county_fips), width=5, side="left",pad="0"))%>% 
    as.tibble() %>% 
    # keep cbsa since it's helpful to contextualize CZ codes
    select(1:5,cbsa10, cbsa_name)
  
  # ERS 90 codes in Willian&Mary PUMA CZ crosswalk and USDA ERS are matching perfectly 
  puma_cz_cross_2010 %>%
    distinct(cz90) %>% 
    mutate(inpuma=TRUE) %>% 
    inner_join(cnty_to_czone_1990 %>% 
                 distinct(cz90=ers90) %>% 
                 mutate(incounty=TRUE)) 
  
  # State and CBSA of refereence for each Commuting zone
  # Copied from parent repository on 2021-12
  # Parent repository: Migrants_workforce
  # Script of conception: Labor_shortage_metrics_III.R
  czone_main_state90<-read_csv("raw/crosswalks/ERS90_states/czone_ERS_main_state90.csv")
  
  
  
  # title of minor occupational codes 
  # Copied from parent repository on 2021-12
  # Parent repository: Crosswalk/occupations/BLS_crosswalks
  soc_minor_titles<-readxl::read_excel("raw/crosswalks/occupations/BLS_crosswalks/soc_structure_2018.xlsx", 
                                       skip = 5) %>% 
    janitor::clean_names() %>% 
    distinct(minor_group, title=x5) %>% 
    drop_na() %>% 
    mutate(soc_minor=str_remove(minor_group,"-"),
           soc_minor=str_remove_all(soc_minor,"0"))
  
  
  # Title of short digit sectors
  # Copied from parent repository on 2021-12
  # Parent repository: ../Crosswalk/industries/, home made files
  sector_titles<-readxl::read_excel("raw/crosswalks/industries/cbp_naics_17_sub_2_edited_CD.xlsx") %>% 
    mutate(naics_2d=as.character(naics_2)) %>% 
    select(naics_2d, sector=SECTOR, short_sector)
  
  
  # ----------------------------------------------------------------------------------#
  # Section 1: Get picture of Foreign-Born workforce------
  # ----------------------------------------------------------------------------------#
  
  table_1<-data_19_15 %>% 
    mutate(CITIZEN_II=as.character(haven::as_factor(CITIZEN)),
           CITIZEN_II=ifelse(CITIZEN_II=="N/A","Not foreign born",CITIZEN_II)) %>% 
    group_by(CITIZEN_II) %>% 
    summarise(Labor_force=sum(PERWT*(LABFORCE==2))) %>% 
    ungroup() %>% 
    mutate(Labor_force_share=Labor_force/sum(Labor_force)) %>% 
    gt::gt() %>% 
    gt::tab_header(title="Labor force composition by citizenship in 2019",
                   subtitle = sample_description) %>% 
    gt::fmt_number(columns = "Labor_force") %>% 
    gt::fmt_percent(columns = "Labor_force_share") %>% 
    gt::summary_rows(
      columns = c("Labor_force","Labor_force_share"),
      fns = list(
        total = "sum")
    )
  
  
  
  
  table_2<-data_19_15 %>%
    mutate(BPL_II=case_when(BPL <= 120~ "US and US Possessions",
                            BPL ==210 ~"Central America",
                            BPL==200~"Mexico",
                            BPL==300~"South America",
                            # BPL>=500 & BPL<=599~"Asia",
                            # BPL==600 ~"Africa",
                            # BPL==700 | BPL==710 ~"Africa",
                            TRUE~"Others")) %>% 
    mutate(CITIZEN_II=as.character(haven::as_factor(CITIZEN)),
           CITIZEN_II=ifelse(CITIZEN_II=="N/A","Not foreign born",CITIZEN_II)) %>% 
    group_by(CITIZEN_II,BPL_II) %>% 
    summarise(Labor_force=sum(PERWT*(LABFORCE==2))) %>% 
    ungroup() %>% 
    pivot_wider(names_from = BPL_II, values_from=Labor_force ) %>% 
    gt::gt() %>% 
    gt::tab_header(title="Labor force composition by citizenship and birthplace in 2019",
                   subtitle = sample_description) %>% 
    gt::summary_rows(
      columns = c("Mexico", "Central America","South America","US and US Possessions"),
      fns = list(
        total = ~sum(., na.rm = TRUE)
      )
    )
  
  
  ###Notes:
  # Population of 3,659,015 Central American in 2019.
  # That's close to MPI's 2019 estimate of 3.78 million: https://www.migrationpolicy.org/article/central-american-immigrants-united-states
  
  
  
  # 1.2 Clean ACS ----------------------
  
  data_19_15<-data_19_15 %>% 
    # create puma variable
    mutate(st1 = str_pad(STATEFIP,3,"left",pad="0"),
           puma1=str_pad(PUMA,5,"left", pad="0"),
           stpuma = paste0(st1,puma1),
           stpuma = as.character(as.numeric(stpuma))) %>% 
    # Create variable of 3 digits occupation (if this works then we can try go more granular:SOCXX)
    mutate(OCC2010=as.integer(OCC2010)) %>%
    mutate(soc_minor=substr(as.character(OCCSOC),1,3)) %>%  
    ## rectify 3 minor classifications that are based on 4 digits (new for the SOC 2018 clasiffication)
    ## (15-1200  Computer Occupation,31-1100  Home Health and Personal Care Aides...,51-5100  Printing Workers)
    mutate(soc_minor=case_when(substr(OCCSOC,1,4)=="1512"~"1512",
                               substr(OCCSOC,1,4)=="3111"~"3111",
                               substr(OCCSOC,1,4)=="5151"~"5151",
                               TRUE~soc_minor)) %>% 
    # Create variable of 2-digits sector (if this works then we can try going more granular: NAICSXX)
    mutate(naics_2d=substr(INDNAICS,1,2)) %>% 
    ## remember there are exceptions in the 2 digits hierarchy
    mutate(naics_2d=case_when(naics_2d>=30 & naics_2d<=33~"31",
                              naics_2d>=44 & naics_2d<=45~"44",
                              naics_2d>=48 & naics_2d<=49~"48",
                              TRUE~naics_2d)) %>% 
    # keep relevant variables
    select(GQ, EMPSTAT, EMPSTATD, LABFORCE,AGE, PERWT, COUNTYFIP,CITIZEN,
           stpuma,STATE,STATEFIP,
           BPL,
           soc_minor,naics_2d) %>%
    # filtering for civilian population 
    # EMPSTATD: 13	Armed forces;	14	Armed forces--at work	; 15	Armed forces--with job but not at work
    filter(EMPSTATD!=13 & EMPSTATD!=14 & EMPSTATD!=15) %>% 
    # filtering for non-institutional population 
    filter(GQ!=3) %>% 
    # filtering for working age population 
    filter(AGE>=16 & AGE<=64) %>%
    # Identify Employed
    mutate(EMPSTAT_2=ifelse(EMPSTAT==1, 1, 0)) %>%
    # Identify in labor force (employed or looking for a job)
    mutate(LABFORCE_2=ifelse(LABFORCE==2,1,0)) %>% 
    # Identify Foreing-born to non-american parents (naturalized, not citizem, not citizen but first w/ 1st papers, foreign born non reported)
    mutate(immigrant_broad=CITIZEN %in% c(2,3,4,5)) %>% 
    # Distinguish natives (or natives' sons) from locals 
    mutate(foreign_born= ifelse(BPL > 120 & immigrant_broad==TRUE,"immigrant","Local")) %>% 
    # Identify foreing-born to non-american parents with detail
    mutate(foreing_born_det_1=case_when(BPL==210 &  immigrant_broad==TRUE~"Central America",
                                        BPL==200 &  immigrant_broad==TRUE~"Mexico",
                                        BPL==300 &  immigrant_broad==TRUE~"South America",
                                        BPL>=400 & BPL<=499 & immigrant_broad==TRUE~"Europe",
                                        BPL>=500 & BPL<=599 & immigrant_broad==TRUE~"Asia",
                                        BPL==600 ~"Africa",
                                        (BPL==700 | BPL==710) &  immigrant_broad==TRUE ~"Africa",
                                        immigrant_broad==TRUE~"Other",
                                        TRUE~"Local"))
  # ----------------------------------------------------------------------------------#
  #2. Build data sets of foreign born workers--------------------------
  # ----------------------------------------------------------------------------------#
  
  #2.x Using County to CZ crosswalk-------------------------
  ## Counties don't map well to CZONES, ignore this step and jump to the next instead
  ## The code of this step was moved to the end of the script, just for reference
  
  # ----------------------------------------------------------------------------------#
  #2.1 Create master dataset for further aggregation -------------------------
  # ----------------------------------------------------------------------------------#
  # Data set aggregated by geography (puma), job (occupation), and sector (naics)
  
  puma_sector_soc_data<-data_19_15 %>%
    select(stpuma,soc_minor,naics_2d,EMPSTAT_2,LABFORCE_2,PERWT,STATE,STATEFIP) %>% 
    # Aggregate counts by PUMA
    group_by(stpuma,soc_minor,naics_2d ) %>%
    summarize(# add state info,
      state=first(STATE),
      state_fips=first(STATEFIP),
      # now count people (use weight PERWT)
      civ_noninst_pop=sum(PERWT),
      # civ non ist employed
      civ_noninst_pop_employed=sum(EMPSTAT_2*PERWT),
      # civ non inst laborforce
      civ_noninst_labforce=sum(LABFORCE_2*PERWT),
      count=n()) %>% 
    # Now aggregate by foreign status
    left_join(data_19_15 %>%
                # Aggregate counts by PUMA
                group_by(stpuma,soc_minor,naics_2d,foreign_born) %>%
                summarize(civ_noninst_pop=sum(PERWT),
                          civ_noninst_pop_employed=sum(EMPSTAT_2*PERWT),
                          civ_noninst_labforce=sum(LABFORCE_2*PERWT),
                          count=n()) %>% 
                ungroup() %>% 
                pivot_wider(names_from=foreign_born,
                            values_from=c(civ_noninst_pop,civ_noninst_pop_employed,civ_noninst_labforce,count),
                            names_sep="_") %>% 
                janitor::clean_names(),
              by=c("stpuma","soc_minor","naics_2d")) %>% 
    # Now aggregate by "detailed" country of birth
    left_join(data_19_15 %>%
                # Aggregate counts by PUMA
                group_by(stpuma,soc_minor,naics_2d,foreing_born_det_1) %>%
                summarize(civ_noninst_pop_employed=sum(EMPSTAT_2*PERWT),
                          civ_noninst_labforce=sum(LABFORCE_2*PERWT),
                          count=n()) %>% 
                ungroup() %>% 
                pivot_wider(names_from=foreing_born_det_1,
                            values_from=c(civ_noninst_pop_employed,civ_noninst_labforce,count),
                            names_sep="_") %>% 
                # only keep south america and central america
                select(stpuma,soc_minor,naics_2d,contains("merica"), contains("Mexico")) %>% 
                janitor::clean_names(),
              by=c("stpuma","soc_minor","naics_2d")) %>%
    mutate(EPOP=(civ_noninst_pop_employed/civ_noninst_pop)*100)
  
  
  print(paste0("Saving data on PUMAS-Sectors, and SOC minors at: ",
               "data/acs_output/puma_sector_soc_acs_data_",first_last_year,".csv"))
  write_csv(puma_sector_soc_data,
            paste0("data/acs_output/puma_sector_soc_acs_data_",first_last_year,".csv"))
  
  ##2.2 Aggregating by Place----------------------------
  
  # 2.2.1 by Commuting zone ERS 90----------------------
  cz_acs_data<-puma_sector_soc_data %>%
    # aggregate by puma and then crosswalk to cz
    group_by(stpuma) %>% 
    summarize_at(vars(starts_with("civ_noninst_pop"),
                      # civ non ist employed
                      starts_with("civ_noninst_pop_employed"),
                      # civ non inst laborforce
                      starts_with("civ_noninst_labforce"),
                      # count of sample size
                      starts_with("count")),
                 funs(sum(.,na.rm =  T))) %>%
    left_join(puma_cz_cross_2010, by=c("stpuma"="stpuma")) %>%
    # perfect match, but remember than a single puma can be splitted into multiple CZ
    drop_na() %>%
    group_by(cz90 ) %>%
    # aggregae by CZ
    summarize_at(vars(starts_with("civ_noninst_pop"),
                      # civ non ist employed
                      starts_with("civ_noninst_pop_employed"),
                      # civ non inst laborforce
                      starts_with("civ_noninst_labforce"),
                      # count of sample size
                      starts_with("count")),
                 # weigth by the share of a PUMA's pop allocated into each CZ
                 funs(sum(.*county_prop_inpuma ))) %>%
    mutate(EPOP=(civ_noninst_pop_employed/civ_noninst_pop)*100) %>% 
    # give some context to 
    left_join(czone_main_state90 %>% 
                rename(cz90=ers90), by="cz90")
  
  print(paste0("Saving data by CZ at ",
               "data/acs_output/cz_acs_data_",first_last_year,".csv"))
  write_csv(cz_acs_data,paste0("data/acs_output/cz_acs_data_",first_last_year,".csv"))
  
  puma_sector_soc_data$civ_noninst_pop %>% sum()
  cz_acs_data$civ_noninst_pop %>% sum() # no data lost
  
  # 2.2.2 by State----------------------
  state_acs_data<-puma_sector_soc_data %>%
    # aggregate by puma and then crosswalk to cz
    group_by(state_fips,state) %>% 
    summarize_at(vars(starts_with("civ_noninst_pop"),
                      # civ non ist employed
                      starts_with("civ_noninst_pop_employed"),
                      # civ non inst laborforce
                      starts_with("civ_noninst_labforce"),
                      # count of sample size
                      starts_with("count")),
                 funs(sum(.,na.rm =  T))) %>%
    mutate(EPOP=(civ_noninst_pop_employed/civ_noninst_pop)*100)
  
  
  print(paste0("Saving data by State at ",
               "data/acs_output/state_acs_data_",first_last_year,".csv"))
  write_csv(state_acs_data,paste0("data/acs_output/state_acs_data_",first_last_year,".csv"))
  
  puma_sector_soc_data$civ_noninst_pop %>% sum()
  state_acs_data$civ_noninst_pop %>% sum() # no data lost
  
  
  
  ## 2.3 Aggregate by Place and Occupation (only employed)-----
  ### Remember there are unemployed by occupation and industry 
  ### bc ACS would ask for last occupation held when employed. 
  #### However, this isn't necessarily the gig the job seeker is applying to
  
  #2.3.1 SOC minor by Commuting zones ERS 1990----------------------
  cz_soc_minor_acs_data<-puma_sector_soc_data %>%
    # aggregate by puma and then crosswalk to cz
    group_by(stpuma,soc_minor) %>% 
    summarize_at(vars(# civ non ist employed
      starts_with("civ_noninst_pop_employed"),
      # count of sample size
      starts_with("count")),
      funs(sum(.,na.rm =  T))) %>%
    left_join(puma_cz_cross_2010, by=c("stpuma"="stpuma")) %>%
    # perfect match, but remember than a single puma can be splitted into multiple CZ
    drop_na() %>%
    group_by(cz90,soc_minor ) %>%
    # aggregae by CZ
    summarize_at(vars(# civ non ist employed
      starts_with("civ_noninst_pop_employed"),
      # count of sample size
      starts_with("count")),
      # weigth by the share of a PUMA's pop allocated into each CZ
      funs(sum(.*county_prop_inpuma ,na.rm = T))) %>% 
    # attach context for Czones
    left_join(czone_main_state90 %>% 
                rename(cz90=ers90), by="cz90")
  
  puma_sector_soc_data$civ_noninst_pop_employed %>% sum()
  cz_soc_minor_acs_data$civ_noninst_pop_employed %>% sum() # no data lost
  
  # add titles and drop non-matched occupations
  cz_soc_minor_acs_data<-cz_soc_minor_acs_data %>% 
    # the only occupation omitted from the list are: Other Military occupations (559), Other occupations (999), no occupation (0)
    # These are omitted.
    inner_join(soc_minor_titles,by="soc_minor") %>% 
    # Let's also remove military occupations (startiing in 55, since ACS doesn't capture much employment here)
    filter(substr(soc_minor,1,2)!=55)
  
  print(paste0("Saving data by CZ and SOC minor at ",
               "data/acs_output/cz_soc_minor_acs_data_",first_last_year,".csv"))
  write_csv(cz_soc_minor_acs_data,
            paste0("data/acs_output/cz_soc_minor_acs_data_",first_last_year,".csv"))
  
  
  #2.3.2 SOC minor by State----------------------
  state_soc_minor_acs_data<-puma_sector_soc_data %>%
    # aggregate by puma and then crosswalk to cz
    group_by(state_fips,state,soc_minor) %>% 
    summarize_at(vars(# civ non ist employed
      starts_with("civ_noninst_pop_employed"),
      # count of sample size
      starts_with("count")),
      funs(sum(.,na.rm =  T))) 
  
  puma_sector_soc_data$civ_noninst_pop_employed %>% sum()
  state_soc_minor_acs_data$civ_noninst_pop_employed %>% sum() # no data lost
  
  # add titles and drop non-matched occupations
  state_soc_minor_acs_data<-state_soc_minor_acs_data %>% 
    # the only occupation omitted from the list are: Other Military occupations (559), Other occupations (999), no occupation (0)
    # These are omitted in this list
    inner_join(soc_minor_titles,by="soc_minor") %>% 
    # Let's also remove military occupations (startiing in 55, since ACS doesn't capture much employment here)
    filter(substr(soc_minor,1,2)!=55)
  
  print(paste0("Saving data by State and Soc minor at ",
               "data/acs_output/state_soc_minor_acs_data_",first_last_year,".csv"))
  write_csv(state_soc_minor_acs_data,
            paste0("data/acs_output/state_soc_minor_acs_data_",first_last_year,".csv"))
  
  ##2.4 Aggregate by Occupation--------------------------
  soc_minor_acs_data<-puma_sector_soc_data %>%
    # aggregate by soc minor
    group_by(soc_minor) %>%
    summarize_at(vars(# civ non ist employed
      starts_with("civ_noninst_pop_employed"),
      # count of sample size
      starts_with("count")),
      # weigth by the share of a PUMA's pop allocated into each CZ
      funs(sum(.,na.rm = T ))) 
  
  puma_sector_soc_data$civ_noninst_pop_employed %>% sum()
  soc_minor_acs_data$civ_noninst_pop_employed %>% sum() # no data lost
  
  # add titles and drop non-matched occupations
  soc_minor_acs_data<-soc_minor_acs_data %>% 
    # the only occupation omitted from the list are: Other Military occupations (559), Other occupations (999), no occupation (0)
    # These are omitted.
    inner_join(soc_minor_titles,by="soc_minor") %>% 
    # Let's also remove military occupations (startiing in 55, since ACS doesn't capture much employment here)
    filter(substr(soc_minor,1,2)!=55)
  
  print(paste0("Saving data by SOC minor at " ,
               "data/acs_output/soc_minor_acs_data_",first_last_year,".csv"))
  write_csv(soc_minor_acs_data,
            paste0("data/acs_output/soc_minor_acs_data_",first_last_year,".csv"))
  
  
  
  ##2.5 Aggregate by Place and Sector (only employment)--------------------------
  
  #2.5.1 by Commuting zones ERS 1996----------------------
  cz_sector_acs_data<-puma_sector_soc_data %>%
    # aggregate by puma and then crosswalk to cz
    group_by(stpuma,naics_2d) %>% 
    summarize_at(vars(# civ non ist employed
      starts_with("civ_noninst_pop_employed"),
      # count of sample size
      starts_with("count")),
      funs(sum(.,na.rm =  T))) %>%
    left_join(puma_cz_cross_2010, by=c("stpuma"="stpuma")) %>%
    # perfect match, but remember than a single puma can be splitted into multiple CZ
    drop_na() %>%
    group_by(cz90,naics_2d ) %>%
    # aggregae by CZ
    summarize_at(vars(
      # civ non ist employed
      starts_with("civ_noninst_pop_employed"),
      # count of sample size
      starts_with("count")),
      # weigth by the share of a PUMA's pop allocated into each CZ
      funs(sum(.*county_prop_inpuma ,na.rm = T))) %>% 
    # attach context for Czones
    left_join(czone_main_state90 %>% 
                rename(cz90=ers90), by="cz90")
  
  puma_sector_soc_data$civ_noninst_pop_employed %>% sum()
  cz_sector_acs_data$civ_noninst_pop_employed %>% sum() # no data lost
  
  cz_sector_acs_data<-cz_sector_acs_data %>% 
    # only 3S (Not specified industries) and 4S (Not specified retail trade) are excluded
    # because lack of correspondence
    # we loose less than 1M jobs
    inner_join(sector_titles) 
  
  print( paste0("Saving data by CZ-Sector at ","data/acs_output/CZ_sector_acs_data_",first_last_year,".csv"))
  
  write_csv(cz_sector_acs_data,
            paste0("data/acs_output/cz_sector_acs_data_",first_last_year,".csv"))
  
  #2.5.2 by State----------------------
  state_sector_acs_data<-puma_sector_soc_data %>%
    # aggregate by puma and then crosswalk to cz
    group_by(state_fips,state,naics_2d) %>% 
    summarize_at(vars(# civ non ist employed
      starts_with("civ_noninst_pop_employed"),
      # count of sample size
      starts_with("count")),
      funs(sum(.,na.rm =  T))) 
  
  
  puma_sector_soc_data$civ_noninst_pop_employed %>% sum()
  state_sector_acs_data$civ_noninst_pop_employed %>% sum() # no data lost
  
  state_sector_acs_data<-state_sector_acs_data %>% 
    # only 3S (Not specified industries) and 4S (Not specified retail trade) are excluded
    # because lack of correspondance
    # we loose less than 1M jobs
    inner_join(sector_titles) 
  
  print(paste0("Saving data by state-sector at",
               "data/acs_output/state_sector_acs_data_",first_last_year,".csv"))
  write_csv(state_sector_acs_data,
            paste0("data/acs_output/state_sector_acs_data_",first_last_year,".csv"))
  
  
  
  ##2.6 Aggregate by  Sector (only employment)--------------------------
  sector_acs_data<-puma_sector_soc_data %>%
    group_by(naics_2d) %>% 
    # aggregae by CZ
    summarize_at(vars(
      # civ non ist employed
      starts_with("civ_noninst_pop_employed"),
      # count of sample size
      starts_with("count")),
      # weigth by the share of a PUMA's pop allocated into each CZ
      funs(sum(. ,na.rm = T))) 
  
  puma_sector_soc_data$civ_noninst_pop_employed %>% sum()
  sector_acs_data$civ_noninst_pop_employed %>% sum() # no data lost
  
  sector_acs_data<-sector_acs_data %>% 
    # only 3S (Not specified industries) and 4S (Not specified retail trade) are excluded
    # because lack of correspondence
    inner_join(sector_titles) 
  
  print(paste0("Saving data by sectors at ",
               "data/acs_output/sector_acs_data_",first_last_year,".csv"))
  write_csv(sector_acs_data,
            paste0("data/acs_output/sector_acs_data_",first_last_year,".csv"))
  
}
## END
### The evaluation of these files is being carried out in the code_Analysis folder

# #2.1 Using County to CZ crosswalk-------------------------
# ## Counties don't map well to CZONES, ignore this file and use the next instead
# ## Code to prove this was dropped in at the end of the script, for reference
# county_data <- data_19_15 %>%
#   select(GQ, EMPSTAT, EMPSTATD, LABFORCE,AGE, PERWT, COUNTYFIP,STATEFIP,STATE,BPL) %>%
#   # filtering for civilian population 
#   filter(EMPSTATD!=13 & EMPSTATD!=14 & EMPSTATD!=15) %>% 
#   # filtering for noninstitutional population 
#   filter(GQ!=3) %>% 
#   # filtering for working age population 
#   filter(AGE>=16 & AGE<=64) %>%
#   # Identify Employed
#   mutate(EMPSTAT_2=ifelse(EMPSTAT==1, 1, 0)) %>%
#   # Identify in laborforce (employed or looking for a job)
#   mutate(LABFORCE_2=ifelse(LABFORCE==2,1,0)) %>% 
#   # Identify Foreing-born
#   mutate(foreign_born= (BPL > 120)) %>% 
#   # Identify foreing-born with detail
#   mutate(camerican=ifelse(BPL==210, 1, 0)) %>%
#   mutate(mexican=ifelse(BPL==200, 1, 0)) %>%
#   mutate(samerican=ifelse(BPL==300, 1, 0)) %>%
#   # Aggregate counts
#   group_by(COUNTYFIP,STATE,STATEFIP) %>%
#   summarize(civ_noninst_pop=sum(PERWT),
#             # civ non ist employed
#             civ_noninst_pop_employed=sum(EMPSTAT_2*PERWT),
#             civ_noninst_pop_employed_foreign=sum(EMPSTAT_2*PERWT*foreign_born),
#             civ_noninst_pop_employed_ca=sum(EMPSTAT_2*PERWT*camerican),
#             civ_noninst_pop_employed_mx=sum(EMPSTAT_2*PERWT*mexican),
#             civ_noninst_pop_employed_sa=sum(EMPSTAT_2*PERWT*samerican),
#             # civ non inst laborforce
#             civ_noninst_labforce=sum(LABFORCE_2*PERWT),
#             civ_noninst_labforce_foreign=sum(LABFORCE_2*PERWT*foreign_born),
#             civ_noninst_labforce_ca=sum(LABFORCE_2*PERWT*camerican),
#             civ_noninst_labforce_mx=sum(LABFORCE_2*PERWT*mexican),
#             civ_noninst_labforce_sa=sum(LABFORCE_2*PERWT*samerican),
#             # count of sample size
#             count_foreign=sum(foreign_born),
#             count_mx=sum(mexican),
#             count_ca=sum(camerican),
#             count_sa=sum(samerican)) %>%
#   mutate(EPOP=(civ_noninst_pop_employed/civ_noninst_pop)*100)
# write_csv(county_data,"data/acs_output/county_acz_acs_data.csv")
# 
# cz_acs_data_1<-county_data %>% 
#   left_join(cnty_to_czone_1990, by=c("COUNTYFIP"="county_fips")) %>%
#   # all the NAs are from countyfips codes that end in 000
#   # We loose several Conmmuting zones here
#   drop_na() %>%
#   group_by(out10) %>%
#   # aggregae by CZ
#   summarize_at(vars(civ_noninst_pop,
#                     # civ non ist employed
#                     civ_noninst_pop_employed,civ_noninst_pop_employed_foreign,
#                     civ_noninst_pop_employed_ca,civ_noninst_pop_employed_mx,
#                     civ_noninst_pop_employed_sa,
#                     # civ non inst laborforce
#                     civ_noninst_labforce,civ_noninst_labforce_foreign,
#                     civ_noninst_labforce_ca,civ_noninst_labforce_mx,
#                     civ_noninst_labforce_sa,
#                     # count of sample size
#                     count_foreign,count_mx,count_ca,
#                     count_sa),sum) %>%
#   mutate(EPOP=(civ_noninst_pop_employed/civ_noninst_pop)*100)
# 
# # many counties didn't match (the non spcified ones)
# county_data %>% 
#   anti_join(cnty_to_czone_1990, by=c("COUNTYFIP"="county_fips")) %>%
#   distinct(COUNTYFIP)
# 
# # 739 CZ didn't matched
# cnty_to_czone_1990 %>% 
#   anti_join(county_data, by=c("county_fips"="COUNTYFIP")) %>%
#   distinct(out90)
# 
# ## Counties don't map well to CZONES, ignore this file and use the next instead
# write_csv(cz_acs_data_1,"data/acs_output/cz_acs_data_1.csv")
