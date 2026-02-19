# UGRC Palletjack Skid Starter Tempalate

[![Push Events](https://github.com/agrc/skid/actions/workflows/push.yml/badge.svg)](https://github.com/agrc/skid/actions/workflows/push.yml)

A template for building skids that use palletjack to update AGOL data from a tabular data source and that are run as Google Cloud Functions

For an example of a working skid, see [erap-skid](https://github.com/agrc/erap-skid) or [uorg-skid](https://github.com/agrc/uorg-skid).

## Creating a Git Repository

The first step in creating a skid is to create a new repository in GitHub that uses this repo as a template. You'll then clone the repo to your computer and start developing.

1. Create a new repo in <https://github.com/agrc>
   - Under `Repository template`, choose `agrc/skid`
   - Name it `projectname-skid` so that everyone knows it's a skid
   - Make it a Public repo
   - Leave everything else alone (the template will take care of it all).
1. Clone the repo on your local computer
   - Use GitHub Desktop or any terminal with the git cli installed.
   - git cli commands:
     - `cd c:\where\you\store\git\repos`
     - `git clone https://github.com/agrc/projectname-skid`

## Initial Skid Development

You'll need to do a few steps to set up your environment to develop a skid. You may also want to make sure you've got the skid working locally before adding the complexity of the cloud function.

This all presumes you're working in Visual Studio Code.

1. Create new environment for the project and install Python
   - `conda create --name PROJECT_NAME python=3.11`
   - `conda activate PROJECT_NAME`
1. Open the repo folder in VS Code
1. Rename `src/skidname` folder to your desired skid name
1. Edit the `setup.py:name, url, description, keywords, and entry_points` to reflect your new skid name
1. Edit the `test_skidname.py` to match your skid name.
   - You will have one `test_filename.py` file for each program file in your `src` directory and you will write tests for the specific file in the `test_filename.py` file
1. Install the skid in your conda environment as an editable package for development
   - This will install all the normal and development dependencies (palletjack, supervisor, etc)
   - `cd c:\path\to\repo`
   - `pip install -e .[tests]`
   - add any additional project requirements to the `setup.py:install_requires` list
1. Set config variables and secrets
   - `secrets.json` holds passwords, secret keys, etc, and will not (and should not) be tracked in git
   - `config.py` holds all the other configuration variables that can be publicly exposed in git
   - Copy `secrets_template.json` to `secrets.json` and change/add whatever values are needed for your skid
   - Change/add variables in `config.py` as needed
1. Write your skid-specific code inside `process()` in `main.py`
   - If it makes your code cleaner, you can write other methods and call them within `process()`
   - Any `print()` statements should instead use `module_logger.info/debug`. The loggers set up in the `_initialize()` method will write to both standard out (the terminal) and to a logfile.
   - Add any captured statistics (number of rows updated, etc) to the `summary_rows` list near the end of `process()` to add them to the email message summary (the logfile is already included as an attachment)
1. Run the tests in VS Code
   - Testing -> Run Tests
1. When you're ready to do a Pull Request, update the version numbering in `version.py`
   - Use [semantic versioning](https://semver.org/#summary)
   - Your first release should be 1.0.0

## Running it as a Google Cloud Function

### Run Locally with Functions Framework

`functions-framework` allows you to run your code in a local framework that mirrors the Google Cloud Functions environment. This lets you make sure it's configured to run properly when called through the cloud process. If you keep the framework of this template, this should start running just fine.

1. Navigate to the package folder within `src`:
   - `cd c:\path\to\repo\src\skidname`
1. Start the local functions framework server. This will attempt to load the function and prepare it to be run, but doesn't actually call it.
   - `functions-framework --target=main --signature-type=event`
1. Open a bash shell (`git-bash` if you installed git for Windows) and run the pubsub.sh script to call the function itself with an HTTP request via curl:
   - `/c/path/to/repo/pubsub.sh`
   - It has to be a bash shell, I can't figure out how to get cmd.exe to send properly-formatted JSON

The bash shell will return an HTTP response. The other terminal you used to run functions-framework should show anything you sent to stdout/stderr (print() statements, logging to console, etc) for debugging purposes

If you make changes to your code, you need to kill (ctrl-c) and restart functions-framework to load them.

### Setup Cloud Dev/Prod Environments in Google Cloud Platform

Skids run as Cloud Functions triggered by Cloud Scheduler sending a notification to a pub/sub topic on a regular schedule.

Work with the GCP maestros to set up a Google project via terraform. They can use the erap configuration as a starting point. Skids use some or all of the following GCP resources:

- Cloud Functions (executes the python)
- Cloud Storage (writing the data files and log files for mid-term retention)
  - Set a data retention policy on the storage bucket for file rotation (90 days is good for a weekly process)
- Cloud Scheduler (sends a notification to a pub/sub topic)
- Cloud Pub/Sub (creates a topic that links Scheduler and the cloud function)
- Secret Manager
  - A `secrets.json` with the requisite login info
  - A `known_hosts` file (for loading from sftp) or a service account private key file (for loading from Google Sheets)

### Setup GitHub CI Pipeline

Skids use a GitHub action to deploy the function, pub/sub topic, and scheduler action to the GCP project. They use the following GitHub secrets to do this:

- Identity provider
- GCP service account email
- Project ID
- Storage Bucket ID

The cloud functions may need 512 MB or 1 GB of RAM to run successfully. The source dir should point to `src/skidname`. A cloud function just runs the specified function in the `main.py` file in the source dir; it doesn't pip install the function itself. It will pip install any dependencies listed in `setup.py`, however.

### Handling Secrets and Configuration Files

Skids use GCP Secrets Manager to make secrets available to the function. They are mounted as local files with a specified mounting directory (`/secrets`). In this mounting scheme, a folder can only hold a single secret, so multiple secrets are handled via nesting folders (ie, `/secrets/app` and `secrets/ftp`). These mount points are specified in the GitHub CI action workflow.

The `secrets.json` folder holds all the login info, etc. A template is available in the repo's root directory. This is read into a dictionary with the `json` package via the `_get_secrets()` function. Other files (`known_hosts`, service account keys) can be handled in a similar manner or just have their path available for direct access.

A separate `config.py` module holds non-secret configuration values. These are accessed by importing the module and accessing them directly.
