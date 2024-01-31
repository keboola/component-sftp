# SFTP writer

SFTP writer component allowing writing tables and files from KBC storage to SFTP repository. Pkey and password authentication 
methods are supported.

**Table of contents:**  
  
[TOC]

# Functionality

The components allows to write tables or files from the KBC Storage to any SFTP repository.

**NOTE** that only the latest files matching the specified tag will be uploaded.

# Configuration
 
## SFTP host URL

Your SFTP host URL

## SFTP host port

SFTP port - by default `22`.

## SFTP user name

Your SFTP user name.

## SFTP password

Password of the SFTP user. Use if you wan't to use the password authentication method.

## SSH private key

Your SSH private key, including the `BEGIN RSA ..` part. If used, the password is ignored.

## Algorithms to disable (optional)

The disabled_algorithms for example: "{'pubkeys': ['rsa-sha2-256', 'rsa-sha2-512']}" Find additional details at [Paramiko documentation](https://docs.paramiko.org/en/latest/api/transport.html?highlight=disabled_algorithms).

## Banner timeout (optional)

Timeout in seconds to wait for SSH banner, default value is 15. This parameter can solve issues related to establishing a connection, caused, for example, by server overload or high network latency.



### Remote destination path

Remote destination path. e.g. existing remote folder, where all files will be stored. ex. `/home/user/mysftpfolder/`

### Append timestamp

If set to true, a current timestamp will be appended to the resulting file, e.g. `test.csv` will be stored as 
 `test_YYYYMMDDHHMiSS.csv`

 
# Development
 
This example contains runnable container with simple unittest. For local testing it is useful to include `data` folder in the root
and use docker-compose commands to run the container or execute tests. 

If required, change local data folder (the `CUSTOM_FOLDER` placeholder) path to your custom path:
```yaml
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
```

Clone this repository, init the workspace and run the component with following command:

```
git clone https://bitbucket.org:kds_consulting_team/kds-team.wr-simple-sftp.git my-new-component
cd my-new-component
docker-compose build
docker-compose run --rm dev
```

Run the test suite and lint check using this command:

```
docker-compose run --rm test
```

# Integration

For information about deployment and integration with KBC, please refer to the [deployment section of developers documentation](https://developers.keboola.com/extend/component/deployment/) 