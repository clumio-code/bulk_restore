# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from botocore.exceptions import ClientError
import boto3
import json
from clumio_sdk_v13 import RetrieveTask


def lambda_handler(events, context):
    """Handle the lambda function to retrieve the S3 restore task."""
    bear = events.get('bear', None)
    base_url = events.get('base_url', None)
    inputs = events.get("inputs", {})
    task = inputs.get('task', None)
    debug = int(events.get('debug', None))

    if not bear:
        bearer_secret = "clumio/token/bulk_restore"
        secretsmanager = boto3.client('secretsmanager')
        try:
            secret_value = secretsmanager.get_secret_value(SecretId=bearer_secret)
            secret_dict = json.loads(secret_value['SecretString'])
            bear = secret_dict.get('token', None)
        except ClientError as e:
            error = e.response['Error']['Code']
            error_msg = f"Describe Volume failed - {error}"
            return {"status": 411, "msg": error_msg}

    if task:
        task_id = task
    else:
        return {"status": 402, "msg": "no task id", "inputs": inputs}

    # Initiate the Clumio API client.
    retrieve_task_api = RetrieveTask()
    if base_url:
        retrieve_task_api.set_url_prefix(base_url)
    retrieve_task_api.set_token(bear)
    retrieve_task_api.set_debug(debug)

    # Retrieve the task status.
    [complete_flag, status, _] = retrieve_task_api.retrieve_task_id(task_id, "one")
    if complete_flag and status != "completed":
        return {"status": 403, "msg": f"task failed {status}", "inputs": inputs}
    elif complete_flag and status == "completed":
        return {"status": 200, "msg": "task completed", "inputs": inputs}
    else:
        return {"status": 205, "msg": f"task not done - {status}", "inputs": inputs}