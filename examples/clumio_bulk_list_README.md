# Clumio Bulk Restore Automation

> [!IMPORTANT]
> Copyright 2024, Clumio, a Commvault Company.
> Licensed under the Apache License, Version 2.0 (the "License");
> you may not use this file except in compliance with the License.
> You may obtain a copy of the License at
>    http://www.apache.org/licenses/LICENSE-2.0
> Unless required by applicable law or agreed to in writing, software
> distributed under the License is distributed on an "AS IS" BASIS,
> WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
> See the License for the specific language governing permissions and
> limitations under the License.

## What is Bulk List
Bulk list is used to list the latest backups of all of the assets in a given accounts. The output of
this state machine could be directly inputted to the Bulk Restore state machin, eliminating the needs
to list it manually.

## Files in this source repository relating to Bulk List.

> [!NOTE]
> JSON file example_step_function_inputs.json is an example of the inputs
> required to run the step function. These inputs would be modified to reflect
> your environment.


> [!NOTE]
> An IAM role that has permissions to execute the step function and the lambda
> functions (and to write to CloudWatch for logging purposes) must be
> identified/created before you deploy the CFT template. If required, you can
> modify the permission of this IAM role after all of the resources have been
> created to scope those permissions to achieve least privilege. If you use the
> AWS secret to store your Clumio api token, this IAM Role will also need to
> have read access to the secret.
> 
> The example of both the role trusted relationships and the policy can be found
> in this folder.

> [!NOTE]
> The `clumio_bulk_list_deploy_cft.yaml` file is the CloudFormation (CFT)
> deployment template. Deploy this CFT template to setup the solution.

## Build

To build you will need a Unix type shell (`bash`, `zsh`, ...), Python 3.12, `make` and `zip`.

```bash
make build
```

It will fetch the dependencies and generate the zip file `clumio_bulk_restore.zip`
under the `build` directory alongside the `clumio_bulk_restore_deploy_cft.yaml`
CloudFormation template.

The zip file must be uploaded to a S3 bucket where it can be accessed by the
CloudFormation Template when you deploy the solution.

## Running the Automation
> [!TIP]
> - [ ] Clumio backups must exist (and not be expired) for all resources that are to be restored.
> - [ ] Identify a S3 bucket where zip file can be copied.
> - [ ] Identify an IAM Role that has the ability to run both the lambda functions and the state machine.
> - [ ] Copy ZIP file from the git repository to the S3 bucket.
> - [ ] Run the CFT YAML file.  You will need to enter the S3 bucket and IAM role as parameters to run the CFT YAML file.
> - [ ] Create an input JSON file for the state machine based upon the example JSON and the descriptions below.
> - [ ] Execute the State machine and pass it your input JSON.
> - [ ] The output will be a JSON that you can pass directly to Restore

## Input Definitions.
An example of the input can be checked in `clumio_bulk_list_inputs_example.json`. The detail of each
input can be checked in the main `README.md` file, but some of the inputs become optional. The meaning of
each tags:

- [REQUIRED]: Required input.
- [OPTIONAL]: Optional input regardless the target account.
- [IF_DIFF_AC]: Required input if the target account is different from the source account, optional otherwise.
- [IF_SAME_AC]: Required input if the target account is same as the source account, optional otherwise.

When not supplied, optional inputs will inherit the value from the original backup.
