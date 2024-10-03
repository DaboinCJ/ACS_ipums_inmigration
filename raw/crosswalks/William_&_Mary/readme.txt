readme for Crosswalks between PUMAs and CZs

Author: Peter McHenry
Dates: 
	5/22/18: Original file creation

The nearby files are crosswalks between county groups or public-use micro data areas (PUMAs) and 
commuting zones (CZs).  IPUMS Census data include county group or PUMA of respondents, and we want to
perform calculations at the CZ level.  This readme file describes the crosswalks without a lot of 
detail.  For more detail and access to the programs that generate the crosswalks, contact me at
pmchenry@wm.edu.

I got Census population counts by county in the US and county group (or PUMA) definitions from IPUMS 
(www.ipums.org).

I got information about the 1990 definition of Commuting Zones based on crosswalks I downloaded from 
the US Department of Agriculture web page.

The main complication in crosswalking here arises because sometimes a single PUMA overlaps multiple CZs.  
The variables in the crosswalk files identify population shares that overlap.  E.g., a county group whose 
population is 30 percent in CZ A and 70 percent in CZ B will be allocated 30 percent to CZ A and 70 to CZ B,
even though we won’t know which residents in that county group are in which CZ.

The variables in the crosswalk files are:
	statefip: State FIPS code
	puma: county groups in IPUMS for 1980 through 2010 are only unique within states (e.g., there might 
		be a PUMA=1 for Virginia and another PUMA=1 for North Carolina.).
	cz90: the CZ90 code for a CZ90 that shares population with the puma.  The CZ90
		codes are explained in the enclosed document Tolbert_Sizer_CZs.pdf.
	popY: the population in Census year Y in the intersection between (statefip puma) and cz90
	pumapopY: the population in (statefip puma) in Census year Y (parts of the PUMA that share
		population with cz90 plus the rest of the PUMA).
	czpopY: the population in cz90 in Census year Y (parts of the CZ90 that share
		population with (statefip puma) plus the rest of the CZ90).
	county_prop_incz=popY/czpopY
	county_prop_inpuma=popY/pumapopY
	
In many cases, a PUMA is completely enclosed within a CZ, so county_prop_inpuma=1.  Other times, a PUMA 
shares population with multiple CZs.  The way I generate CZ-specific population measures is to assign 
to a CZ the characteristics of its member PUMAs, in proportion to their population shares in the CZ.


========================================
Code for counting populations by CZ using data on populations by IPUMS Census county group or PUMA

Below is code from a program that reads a data set (temp1990all.dta) where each observation is a respondent to 
the 1990 Census.  It keeps only those with high school or less education.  It then uses a collapse command 
(using population weights) to estimate the population of people with this educational attainment in each PUMA.  
It merges in a crosswalk between PUMAs and commuting zones (CZs).  This merge identifies all CZs that overlap 
each PUMA.  The file then assigns each PUMA population to overlapping CZs in proportion to the population overlap 
identified in the crosswalk file.  It ends with a data set where each observation is a CZ and the main variable 
is the estimate of the high school or less population in the CZ.


use temp1990all, clear
keep if lths==1|hs==1
gen puma_N_hsless=1
collapse (sum) puma_N_hsless [pw=perwt], by(statefip puma)
merge 1:m statefip puma using puma_cz_cross_1990
bysort cz90: egen cz_N_hsless=sum(county_prop_inpuma*puma_N_hsless)
by cz90: keep if _n==_N
egen natl_N_hsless=sum(cz_N_hsless)
keep cz90 cz_N_hsless natl_N_hsless
save census1990cz_N_hsless, replace

