# JOB_ME01_gme_offertepubbliche
Public Offers data from GME for all electric markets 

![Version](https://img.shields.io/badge/VERSION-V1-green)
![Owner](https://img.shields.io/badge/OWNER-Somebody_MBS-blue)
![Dev](https://img.shields.io/badge/DEV-Alejandro_Abraham-orange)
![Fluxer](https://img.shields.io/badge/fluxer-yes-yellow)

## Intro

This job retrieves from YYY the something data.

- **Database:** `defaultdb`
- **Field:** FIELD - `ME`
- **Type:** RAW - `01`
- **Frequency:** `1 - time (PT15M-PT60M)`
- **Name**: `Data GME - MERCATO XX`
- **Scripts:**
1. Retrieval
2. Preparation
3. Push

## Data Retrieved

Fro the following markets: 
- **MGP**  
- **MSD**  
- **MB**  
- **MI**  
- **XBID**  


## Main Uses
- Osservatorio
- Scenario


## Documentation
[Loop page link]()

## Github actions
In order to launch virtual machine in Github using action, in `r-scripts-workflow.yml` code you need to add the folliwng chunk of code at the beginning:
```
on:
  push:
    branches:
      - main
```

## Usage

### Install fluxer
```
remotes::install_github('mbsenergy/fluxer')
```

### Docker
Open the Docker daemon (dektop app) and the in the terminal:
```
docker login
docker pull alejandrocerved/alejandrocerved/fluxerdev:4.4.1
```
### VS Code
- Install the Remote extension  
- "Reopen in container" pop up or in the bottom left part, click the >< symbol and select Reopen in container.



### API Keys
Recall to store your API keys in your `.Renviron` and then copying them to the **Github** `secrets`
- **Github**
- **GME User and PSW**

## RENV:
- ignore and excude `fluxer`

```r
renv::settings$ignored.packages(c("fluxer"))
renv::snapshot()
```