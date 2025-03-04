# SFTP Writer
The SFTP component writes tables and files from Keboola Storage to an SFTP repository. Primary key and password authentication 
methods are supported.

**Table of contents:**  
  
[TOC]

# Functionality

The component allows to write tables or files from Keboola Storage to any SFTP repository.

***Note:** Only the latest files matching the specified tag will be uploaded.*

# Configuration
 
## SFTP Host URL

Your SFTP host URL

## SFTP Host Port

SFTP port - by default `22`

## SFTP User Name

Your SFTP user name

## SFTP password

The password of the SFTP user. Use this if you don't want to use the password authentication method.

## SSH Private Key

Your SSH private key, including the `BEGIN RSA ..` part. If used, the password is ignored.

## Algorithms to Disable (Optional)

The disabled_algorithms, for example: "{'pubkeys': ['rsa-sha2-256', 'rsa-sha2-512']}" Find additional details at the [Paramiko documentation](https://docs.paramiko.org/en/latest/api/transport.html?highlight=disabled_algorithms).

## Banner Timeout (Optional)

Timeout in seconds to wait for the SSH banner, default: 15. This parameter can solve issues related to establishing a connection, caused, for example, by server overload or high network latency.



### Remote Destination Path

Remote destination path, e.g., an existing remote folder where all files will be stored. ex. `/home/user/mysftpfolder/`

### Append Timestamp

If set to `true`, a current timestamp will be appended to the resulting file, e.g., `test.csv` will be stored as 
 `test_YYYYMMDDHHMiSS.csv`.

 
# Development
 
This example contains a runnable container with a simple unittest. For local testing, it is useful to include the `data` folder in the root directory
and use Docker Compose commands to run the container or execute tests. 

If required, change the local data folder path (the `CUSTOM_FOLDER` placeholder) to your custom path:
```yaml
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
```

Clone this repository, initialize the workspace, and run the component with the following commands:

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

For details about deployment and integration with Keboola, please refer to the [deployment section of the developer documentation](https://developers.keboola.com/extend/component/deployment/). 
