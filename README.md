# JOB_ME01_gme_OFFERS
This job retrieves XML files via FTP from GME site for all markets offers.

![Version](https://img.shields.io/badge/VERSION-V1-green)
![Owner](https://img.shields.io/badge/OWNER-Somebody_MBS-blue)
![Dev](https://img.shields.io/badge/DEV-Alejandro Abraham-orange)
![Fluxer](https://img.shields.io/badge/fluxer-yes-yellow)




Field: MARKETS- ME
Type: RAW-01
Frequency: INTERVAL
Name: GME Market Offers

Owner: TBD@cerved.com 
Dev: alejandro.abraham@cerved.com 
Version: v1 
Scripts:
Retrieval
Preparation
Push
Log
Main Uses
Scenario
Osservatorio
Consulting - Energy Management
Asset Valutation

Github
https://github.com/mbsenergy/JOB_ME01_gme_prices


Tables

MGP Offers - ME01_gme_mgp_offers
MSD Offers - ME01_gme_msd_offers
MB Offers - ME01_gme_mb_offers
XBID Offers - ME01_gme_xbid_offers



## Documentation
[Loop page link](https://cervedgroup.sharepoint.com/:fl:/r/contentstorage/CSP_f58f06db-6d96-48bc-bfdc-b97f082093eb/Raccolta%20documenti/LoopAppData/DOC_ME01_gme%20OFFERS.loop?d=w7f088d6e3f3c40f992d8c459f9622a67&csf=1&web=1&e=12ZAvV&nav=cz0lMkZjb250ZW50c3RvcmFnZSUyRkNTUF9mNThmMDZkYi02ZDk2LTQ4YmMtYmZkYy1iOTdmMDgyMDkzZWImZD1iJTIxMndhUDlaWnR2RWlfM0xsX0NDQ1Q2Nm1ncHhiNTZZWkVnQnloTU5GR01Cc3E1SlpleTRPTlI3cTU0eWI1UmVZbiZmPTAxQ1NMRTRWVE9SVUVINlBCNzdGQUpGV0dFTEg0V0VLVEgmYz0lMkYmYT1Mb29wQXBwJng9JTdCJTIydyUyMiUzQSUyMlQwUlRVSHhqWlhKMlpXUm5jbTkxY0M1emFHRnlaWEJ2YVc1MExtTnZiWHhpSVRKM1lWQTVXbHAwZGtWcFh6Tk1iRjlEUTBOVU5qWnRaM0I0WWpVMldWcEZaMEo1YUUxT1JrZE5Rbk54TlVwYVpYazBUMDVTTjNFMU5IbGlOVkpsV1c1OE1ERkRVMHhGTkZaU1UwcE9WREpWVjBOWFZVcEhTVGRDU0VKS1dWRXpVRE0wVFElM0QlM0QlMjIlMkMlMjJpJTIyJTNBJTIyMjEyNDgzMDAtNzA5ZS00MzNmLTljNmItMGNlNWRiNzEzMzc3JTIyJTdE)

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