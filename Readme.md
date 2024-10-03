# Processing ACS IPUMS to generate immigration estimates

## Scripts

-   **202109_foreign_born_in_acs_function.R:** Embeds the above file in a function so it's easier to clean ACS IPUMS data sets from other years.

-   **202109_foreign_born_in_acs_runner.R:** Calls the foreign workforce function to clean ACS IPUMS data sets from different years. It produces **panels for total and immigrant employment**. Outputs can be found in ***data/acs_output***. The folder includes 1-year estimates files, 5-year estimates files, and 1-year panel data files, all at the following levels of aggregation:

    -   Commuting zone (CZ) & Sector (naics 2-digits)

    -   Commuting zone (CZ) & Minor occupation (soc 3-digits)

    -   State & Sector (naics 2-digits) d. State & Minor occupation (soc 3-digits)

    -   Commuting zone (CZ) f. State g. Sector (naics 2-digits)

    -   Minor occupation (soc 3-digits)

## Raw data (`/raw`)

-   `/raw/croswalks:` Crosswalks on industry codes, occupation codes, states, and more,

-   `/raw/czone_pennsate:` Commuting zones delineations.

<!-- -->

-   `/raw/acs_ipums:` (NOT IN REPO DUE TO SIZE LIMITS, REQUEST A PRIVATE DATA TRANSFER AT cdaboin2\@gmail.com) Data extracts from [ACS Ipums](https://usa.ipums.org/usa/index.shtml). Datasets come in .dat format; you can find how to read them in the scripts of this repo. Extracts come in .xlm format, which you can open in your browser. These files show you the set of columns included in the extract and the characteristics of the sample and settings. Note that ACS extracts can be based on 1-year or 5-year pooled samples.

### Context

The files presented here are a by-product of multiple research efforts to estimate the prevalence of immigrant workers across regions, occupations, and industries. Some of the articles highlighting those findings are:

-   [A roadmap for immigration reform](https://www.brookings.edu/articles/a-roadmap-for-immigration-reform/)

-   [Immigration as an engine for reviving the middle class in midsized cities](https://www.brookings.edu/articles/immigration-as-an-engine-for-reviving-the-middle-class-in-midsized-cities/)
