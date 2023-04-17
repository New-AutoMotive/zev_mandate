# modules

This repo contains standard modules that we use in many contexts, e.g. the module `connector.py`. The repo is meant to be copied (not cloned!) in any new progect making use of the bash file `download_repo.sh` that can be downloaded [here](https://console.cloud.google.com/storage/browser/credentials_newautomotive;tab=objects?forceOnBucketsSortingFiltering=false&project=rugged-baton-283921&prefix=&forceOnObjectsSortingFiltering=false).

Run `./download-repo.sh <url/to/repo> <subfolder/to/unzip>` to get the content of the repo in a specific folder, for instance `./downlaod-repo.sh http://github.com/New-AutoMotive/modules ./modules` to downlaod this repo and move its content in the folder `modules`. If the latter does not exist is created, otherwise the content of the repo will be moved in the already existent directory.
