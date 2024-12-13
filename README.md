# JOB_XX01_template
API retrieval from somewhere 

![Version](https://img.shields.io/badge/VERSION-V1-green)
![Owner](https://img.shields.io/badge/OWNER-Somebody_MBS-blue)
![Dev](https://img.shields.io/badge/DEV-Someone_FLUX-orange)
![Fluxer](https://img.shields.io/badge/fluxer-yes-yellow)

## Intro

This job retrieves from YYY the something data.

- **Database:** `<MOTHERDUCK_DB>`
- **Field:** FIELD - `XX`
- **Type:** RAW - `01`
- **Frequency:** `1 - month`
- **Name**: `Human readable name`
- **Scripts:**
1. Retrieval
2. Preparation
3. Push
4. Log


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
- **Gmail**
- **Motherduck**
