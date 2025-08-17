import copy
import cdep_utils
import logging
import urllib.parse
import subprocess
import json
import requests
from datetime import timedelta
from global_constants import (
    ansible_venv_path, generic_output_pattern, V8_MAX_SUPPORTED_PARALLELISM, provisioning_venv_path, ansible_playbook_path
)
from charms_s3_connector import CharmsS3Connector
from uco.operators.slack_monitor import SlackMonitorOperator

import airflow
from airflow.models import DAG
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowException, AirflowSkipException, AirflowFailException
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.configuration import conf
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from uco.hooks.aci import ACIHook
from uco.operators.uco_bash_operator import UCOBashOperator
from uco.operators.uco_trigger_dagrun import UCOTriggerDagRunOperator

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class SkippableUCOTriggerDagRunOperator(UCOTriggerDagRunOperator):
    """
    A custom operator that inherits from UCOTriggerDagRunOperator and adds a skipping mechanism.
    If the rendered 'conf' parameter is an empty dictionary, it skips the task.
    """
    def execute(self, context):
        if not self.conf:
            raise AirflowSkipException(f"Skipping task {self.task_id} because configuration was not provided.")
        super().execute(context)


""" Section 1: Global variables : START"""

experience_audit_dag_id = "ucoyoda_acc_experienceAudit"
# Optional parameters
skip_pre_experience_audit = "{{ dag_run.conf['params'].get('skip_pre_experience_audit', 'false') }}"
skip_post_experience_audit = "{{ dag_run.conf['params'].get('skip_post_experience_audit', 'false') }}"

skip_experience_audit = "{{ dag_run.conf['params']['skip_experience_audit'] if 'skip_experience_audit' in dag_run.conf['params'] else 'false'}}"
skip_pre_validation = "{{ dag_run.conf['params']['skip_pre_validation'] if 'skip_pre_validation' in dag_run.conf['params'] else 'false'}}"

environments = ["mid", "rt"]
ansible_playbook_path_fulltechstack = '/opt/acc-core-plugins'

# Configuration variables for pre-checks
app_servers_first_url = "{% set server_list = dag_run.conf['mkt'][0]['hosts'].split(',')|sort -%} {{ server_list[0] }},"
app_servers = "{{ dag_run.conf['mkt'][0]['hosts'] }}" + ","
mid_servers = "{{ dag_run.conf.get('mid', [{}])[0].get('hosts', '') }}"
rt_servers = "{{ dag_run.conf.get('rt', [{}])[0].get('hosts', '') }}"
mid_rt_servers = "{{ [dag_run.conf.get('mid', [{}])[0].get('hosts', ''), dag_run.conf.get('rt', [{}])[0].get('hosts', '')] | select | join(',') + ',' }}"
region = "{{ dag_run.conf['mkt'][0]['region'] }}"
build_version = "{{ dag_run.conf['mkt'][0].get('build', dag_run.conf['mkt'][0].get('target_build', dag_run.conf.get('params', {}).get('target_build', 'default'))) }}"
rds_identifier = "{{ dag_run.conf['mkt'][0]['rdsEndpoint'].split('.')[0].strip() }}"
instance_name = "{{ dag_run.conf['mkt'][0]['instanceName'] }}"
aws_account_id = "{{ dag_run.conf['mkt'][0]['cloudAccountId'] }}"

# Slack notification functions
def get_vault_token():
    return subprocess.Popen(
                        'export AWS_PROFILE=campaign-provisioning; \
                        source /opt/.virtualenvs/provisioning-venv/bin/activate; \
                        source /opt/acc-core-plugins/vault_token.sh; echo $VAULT_TOKEN',
                        shell=True, stdout=subprocess.PIPE, executable='/bin/bash'
    ).stdout.read()[:-1].decode()

def get_slack_token():
    VAULT_URL = "https://vault-amer.adobe.net"
    headers = {
            "X-Vault-Token": get_vault_token(),
    }
    response = requests.request(
            "GET",
            f"{VAULT_URL}/v1/secret/campaign/techops-secrets/tools/techops-tools/scouter_slack",
            headers=headers
    )
    return response.json()["data"]["slack_token"]

def post_message_to_slack(msg, channel):
    slack_client = WebClient(token=get_slack_token())
    try:
        slack_client.chat_postMessage(channel=channel, text=msg)
    except SlackApiError as e:
        raise AirflowFailException("Error: {e}".format(e=e))   

def fail_notification(context):
    dag_id = context.get('task_instance').dag_id
    task_id = context.get('task_instance').task_id
    run_id = context['dag_run'].run_id
    execution_date = context.get('execution_date')
    params = context.get('params')
    query = {
        "dag_id": dag_id,
        "run_id": run_id,
        "execution_date": execution_date
    }
    encoded_url = urllib.parse.urlencode(query)
    url = f"{conf.get('webserver', 'base_url').lower()}/graph?{encoded_url}"
    
    dag_conf = context.get("dag_run").conf
    channel = "#mass-rollout-failures"
    slack_msg = f"""
        :red_circle: V8 Charms Cluster Update Task Failed :arrow_down:
        ---------------------------------------------------------------------------------------------------------------
        |    • *Task_Id*                       :  `{task_id}`                                     
        |    • *Dag_Id*                        :  `{dag_id}`                                                          
        |    • *Run_Id*                        :  `{run_id}`                                                          
        |    • *Instance Name*          :  `{dag_conf.get('mkt', [{}])[0].get('instanceName', '') if dag_conf.get('mkt') else 'unknown'}`,                                      
        |    • *Execution Date*         :  `{execution_date.strftime('%Y-%m-%d')}`                                                         
        |    • *Log Url*                       :  <{url}| DAG Link>                                                   
        ---------------------------------------------------------------------------------------------------------------
    """
    post_message_to_slack(slack_msg, channel)

args = {
    'owner': 'ucoairflow',
    'start_date': airflow.utils.dates.days_ago(0),
    'depends_on_past': False,
    'trigger_rule': 'none_failed',
    'retries': 0,
    'queue': 'acc',
    'pool': 'acc',
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': fail_notification,
}

""" Section 1: Global variables : END"""

""" Section 2: DAG Definition and Custom Filters : START"""


def get_payload_for_first_appserver(conf):
    dag_conf = copy.deepcopy(conf)
    hosts = dag_conf['hosts'].split(',')
    hostIds = dag_conf['hostCloudIds'].split(',')
    host_ids_dict = {host: cloud_id for host, cloud_id in zip(hosts, hostIds)}
    dag_conf['skip_backup'] = dag_conf['skip_backup'] if len(dag_conf.get('skip_backup', "")) > 0 else 'false'
    dag_conf['skip_build_upgrade'] = dag_conf['skip_build_upgrade'] if len(dag_conf.get('skip_build_upgrade', "")) > 0 else 'false'
    hosts = sorted(hosts)
    dag_conf['hosts'] = hosts[0]
    dag_conf['hostCloudIds'] = host_ids_dict[dag_conf['hosts']]
    return dag_conf


def get_payload_for_rest_appservers(conf):
    dag_conf = copy.deepcopy(conf)
    hosts = dag_conf['hosts'].split(',')
    hostIds = dag_conf['hostCloudIds'].split(',')
    host_ids_dict = {host: cloud_id for host, cloud_id in zip(hosts, hostIds)}
    dag_conf['skip_backup'] = dag_conf['skip_backup'] if len(dag_conf.get('skip_backup', "")) > 0 else 'false'
    dag_conf['skip_build_upgrade'] = dag_conf['skip_build_upgrade'] if len(dag_conf.get('skip_build_upgrade', "")) > 0 else 'false'
    hosts = sorted(hosts)
    dag_conf['hosts'] = ','.join(hosts[1:])

    hostCloudIds = [host_ids_dict[host] for host in hosts[1:]]
    dag_conf['hostCloudIds'] = ','.join(hostCloudIds)
    dag_conf['ignoreErrors'] = '901'
    dag_conf['skip_application_pre_checks'] = 'true'
    return dag_conf


def get_experience_audit_payload(conf, task_instance=None, ignore_failure=False, audit_phase="prechange"):
    dag_conf = copy.deepcopy(conf)
    dag_conf['ignoreFailure'] = ignore_failure
    dag_conf['auditPhase'] = audit_phase  # Add phase identifier to distinguish pre vs post

    # Safely get tenant_id from XCom with fallback
    tenant_id = None
    if task_instance:
        try:
            tenant_id = task_instance.xcom_pull(task_ids='gather_and_set_data', key='tenant_id')
        except Exception as e:
            print(f"Warning: Could not retrieve tenant_id from XCom: {e}")

    # Set tenant_id with fallback to customerID if not available
    dag_conf['tenantId'] = tenant_id or conf.get('customerID', 'unknown')

    # Ensure required fields are present with safe defaults
    dag_conf['aciId'] = conf.get('aciID', conf.get('aciId', 'unknown'))
    dag_conf['instanceUrl'] = conf.get('instanceUrl', 'unknown')
    dag_conf['instanceName'] = conf.get('instanceName', 'unknown')

    return dag_conf


def enrich_dag_conf(conf, key, value=None):
    """
    Custom filter to add a new key and value to the configuration for SkippableUCOTriggerDagRunOperator tasks.
    Only adds the key if the original configuration is not empty.

    Args:
        conf: The original configuration
        key: Key to add to the configuration
        value: Value to add to the configuration

    Returns:
        Enhanced configuration with the new key and value if conf is not empty, otherwise empty dict
    """
    if not conf:  # If conf is empty dict {}, return empty dict
        return {}

    dag_conf = copy.deepcopy(conf)
    dag_conf[key] = value

    return dag_conf


dag = DAG(
    dag_id="ucoyoda_acc_v8CharmsClusterUpdate",
    description="Circuit for v8 cluster where execution will be done on all mids and rts in sequential with appserver sequencing",
    schedule_interval=None,
    render_template_as_native_obj=True,
    default_args=args,
    user_defined_filters={
        "getpayloadforfirstappserver": get_payload_for_first_appserver,
        "getpayloadforrestappserver": get_payload_for_rest_appservers,
        "getexperienceauditpayload": get_experience_audit_payload,
        "enrichdagconf": enrich_dag_conf,
    },
)

""" Section 2: DAG Definition and Custom Filters : END"""

""" Section 3: Utility Functions : START"""

def skip_task(do_skip, task_id_no_skip, task_id_skip):
    if do_skip == True or do_skip == "True" or do_skip == "true":
        return task_id_skip
    return task_id_no_skip


def gather_and_set_data(instance_id, **kwargs):
    aci_hook = ACIHook(conn_id="cassini_aci", ims_conn_id="cassini_ims")
    tenant_id = aci_hook.get_instance(instance_id).get("tenant_id")
    configs_with_sf_accounts = aci_hook.list_instance_databaseconfigs(
        instance_id,
        filters=["external_accounts.server~.*snowflakecomputing.*"]
    )
    is_snowflake_enabled = len(configs_with_sf_accounts) > 0
    kwargs['ti'].xcom_push(key="tenant_id", value=tenant_id)
    kwargs['ti'].xcom_push(key="is_snowflake_enabled", value=is_snowflake_enabled)
    return


def create_and_upload_metadata(**context):
    """Create and upload metadata to S3."""
    mkt_config = context["dag_run"].conf["mkt"][0]
    org_id = mkt_config["customerID"]
    tenant_id = context['ti'].xcom_pull(key='tenant_id', task_ids='gather_and_set_data')
    # Handle both aciID and aciId field variations
    aci_id = mkt_config.get("aciID") or mkt_config.get("aciId")
    
    # Validate required parameters
    if not tenant_id:
        raise ValueError("tenant_id not found in XCom from gather_and_set_data task")
    if not aci_id:
        raise ValueError("aciID/aciId not found in MKT configuration")
    
    connector = CharmsS3Connector(
        org_id=org_id,
        tenant=tenant_id,
    )

    # Clear existing data
    connector.clear_data()
    connector.attach_data_from_aci(aci_id)


def gather_cluster_precheck_data(instance_id):
    """Gather cluster data including instances, connections, and hostnames."""
    aci_hook = ACIHook(conn_id='cassini_aci', ims_conn_id='cassini_ims')
    connections = aci_hook.list_instance_connections(instance_id, ["connection_active==true"])
    instance_ids = [c.get("connected_instance") for c in connections if c.get("connected_instance")]
    instance_ids.append(instance_id)
    instances = aci_hook.list_instances(
        filters=["instance_id~(^{}$)".format("$|^".join(instance_ids))]
    )
    hostnames = list()
    for instance in instances:
        appservers = aci_hook.list_instance_appservers(
            instance.get("instance_id")
        )
        for appserver in appservers:
            hostnames.append(appserver.get("hostname"))
    return {"instances": instances, "connections": connections, "hostnames": hostnames}


def pre_check_gather_cluster_data(instance_id, **kwargs):
    """Check if any ACI instances are in stopped state and fail immediately if found."""
    logger.info(f"Starting pre-check for cluster data on instance: {instance_id}")
    
    try:
        cluster_data = gather_cluster_precheck_data(instance_id)
        instances = cluster_data.get("instances", [])
        
        stopped_instances = []
        running_instances = []
        
        for instance in instances:
            instance_id_current = instance.get("instance_id")
            instance_state = instance.get("state", "unknown")
            instance_name = instance.get("name", "unknown")
            
            logger.info(f"Instance {instance_id_current} ({instance_name}): state = {instance_state}")
            
            if instance_state.lower() in ["stopped", "stop", "stopping"]:
                stopped_instances.append({
                    "id": instance_id_current,
                    "name": instance_name,
                    "state": instance_state
                })
            else:
                running_instances.append({
                    "id": instance_id_current,
                    "name": instance_name,
                    "state": instance_state
                })
        
        # Log summary
        logger.info(f"Total instances checked: {len(instances)}")
        logger.info(f"Running instances: {len(running_instances)}")
        logger.info(f"Stopped instances: {len(stopped_instances)}")
        
        # If any instances are stopped, fail the task immediately
        if stopped_instances:
            stopped_details = "\n".join([
                f"  - {inst['name']} (ID: {inst['id']}) - State: {inst['state']}"
                for inst in stopped_instances
            ])
            
            error_msg = f"""
CRITICAL ERROR: Found {len(stopped_instances)} stopped instance(s) in the cluster.
This will cause the V8 Charms Cluster Update to fail.

Stopped instances:
{stopped_details}

All instances must be in running state before proceeding with cluster updates.
Please start the stopped instances and retry the DAG execution.
            """
            
            logger.error(error_msg)
            raise AirflowFailException(error_msg)
        
        logger.info("✅ All instances are in running state. Pre-check passed successfully.")
        return {"status": "success", "running_instances": len(running_instances)}
        
    except Exception as e:
        logger.error(f"Error during cluster pre-check: {str(e)}")
        raise AirflowFailException(f"Cluster pre-check failed: {str(e)}")


def pre_check_active_connections(instance_id, **kwargs):
    """Check if all ACI instance connections are active and fail immediately if any are inactive or failed."""
    cluster_data = gather_cluster_precheck_data(instance_id)
    for connection in cluster_data.get("connections"):
        if connection.get("active", False):
            logging.error(
                "Connection inactive found. \n{}".format(json.dumps(connection))
            )
            raise AirflowFailException("Connection inactive found. {}".format(connection.get("connection_url")))
        if connection.get("connection_check").get("failed"):
            logging.error(
                "Connection check failed. \n{}".format(json.dumps(connection))
            )
            raise AirflowFailException("Connection check failed. {} - {}".format(connection.get("connection_url"), connection.get("connection_check").get("reason")))


def pre_check_rt_mid_count(instance_id, **kwargs):
    """Check if RT and MID instance counts do not exceed maximum allowed (4 each)."""
    cluster_data = gather_cluster_precheck_data(instance_id)
    rt_count = 0
    mid_count = 0
    
    for instance in cluster_data.get("instances"):
        instance_type = instance.get("instance_type")
        if instance_type == "rt":
            rt_count += 1
        elif instance_type == "mid":
            mid_count += 1
    
    if rt_count > 4:
        logging.error(
            "RT instance count ({}) exceeds maximum allowed (4). \n{}".format(
                rt_count, json.dumps([i for i in cluster_data.get("instances") if i.get("instance_type") == "rt"])
            )
        )
        raise AirflowFailException("RT instance count ({}) exceeds maximum allowed (4).".format(rt_count))
    
    if mid_count > 4:
        logging.error(
            "MID instance count ({}) exceeds maximum allowed (4). \n{}".format(
                mid_count, json.dumps([i for i in cluster_data.get("instances") if i.get("instance_type") == "mid"])
            )
        )
        raise AirflowFailException("MID instance count ({}) exceeds maximum allowed (4).".format(mid_count))


""" Section 3: Utility Functions : END"""

""" Section 4: Task Definitions : START"""


def start_dag_message(**context):
    print("DAG started running - ucoyoda_acc_v8CharmsClusterUpdate")
    return "DAG execution initiated successfully"

task_start_run_dag = PythonOperator(
    task_id="start_run_dag",
    python_callable=start_dag_message,
    provide_context=True,
    execution_timeout=timedelta(minutes=1),
    retries=0,
    dag=dag,
)

task_pre_checks_skip = BranchPythonOperator(
    task_id='pre_checks_skip_task',
    python_callable=skip_task,
    provide_context=True,
    op_kwargs={'do_skip': skip_pre_validation,
               'task_id_no_skip': ['pre_check_db_task',
                                   'pre_schema_generation_task',
                                   'pre_check_db_version_task',
                                   'pre_url_service_check',
                                   'pre_validation_task',
                                   'pre_infra_checks_task',
                                   'pre_check_gather_cluster_data_task',
                                   'pre_check_active_connections_task',
                                   'pre_check_rt_mid_count_task',
                                   ],
               'task_id_skip': 'gather_and_set_data'},
    dag=dag,
)

task_pre_schema_generation = BashOperator(
    task_id="pre_schema_generation_task",
    bash_command="source " + provisioning_venv_path + "/bin/activate && " + "ansible-playbook -i " + app_servers_first_url + " " + ansible_playbook_path_fulltechstack + "/os_upgrade/role_master.yml --e 'rolename=db-validation-checks' --e v8_instance_name=" + instance_name + " --tags acc_v8_schema_generation",
    execution_timeout=timedelta(hours=1),
    retries=1,
    retry_delay=timedelta(minutes=2),
    dag=dag,
)

task_pre_check_db_version = BashOperator(
    task_id="pre_check_db_version_task",
    bash_command="source " + provisioning_venv_path + "/bin/activate && " + "ansible-playbook -i " + app_servers + " " + ansible_playbook_path_fulltechstack + "/os_upgrade/role_master.yml --e 'rolename=db-validation-checks' --e v8_instance_name=" + instance_name + " --tags acc_v8_db_version_check",
    execution_timeout=timedelta(hours=1),
    retries=1,
    retry_delay=timedelta(minutes=2),
    dag=dag,
)

task_pre_check_db = BashOperator(
    task_id="pre_check_db_task",
    bash_command="source " + provisioning_venv_path + "/bin/activate && " + "ansible-playbook -i " + app_servers_first_url + " " + ansible_playbook_path_fulltechstack + "/os_upgrade/role_master.yml --e 'rolename=db-validation-checks' --e v8_instance_name=" + instance_name + " --tags acc_v8_db_prechecks",
    execution_timeout=timedelta(hours=1),
    retries=1,
    retry_delay=timedelta(minutes=2),
    dag=dag,
)

task_pre_url_service_check = BashOperator(
    task_id="pre_url_service_check",
    bash_command="source " + provisioning_venv_path + "/bin/activate && " + "ansible-playbook -i " + app_servers + " " + ansible_playbook_path_fulltechstack + "/os_upgrade/role_master.yml  --tags acc_v8_pre_infra_check --e 'ansible_python_interpreter=/usr/bin/python3'" + " --e 'rolename=url-service-check'",
    execution_timeout=timedelta(hours=1),
    retries=1,
    retry_delay=timedelta(minutes=2),
    dag=dag,
)

task_pre_infra_checks = BashOperator(
    task_id="pre_infra_checks_task",
    bash_command="source " + provisioning_venv_path + "/bin/activate && export AWS_PROFILE=uco-cross-account && " + "ansible-playbook -i " + app_servers + " " + ansible_playbook_path_fulltechstack + "/os_upgrade/role_master.yml --tags acc_v8_pre_infra_check --e 'rolename=infra-checks' --e 'rds_identifier=" + rds_identifier + "'" + " --e 'aws_region=" + region + "'  --e 'aws_account_id=" + aws_account_id + "'",
    execution_timeout=timedelta(hours=1),
    retries=1,
    retry_delay=timedelta(minutes=2),
    dag=dag,
)

task_pre_validations = BashOperator(
    task_id="pre_validation_task",
    bash_command="source " + provisioning_venv_path + "/bin/activate && " + "ansible-playbook -i " + app_servers + " " + ansible_playbook_path_fulltechstack + "/os_upgrade/role_master.yml --e 'rolename=validation' --e 'product=acc' --e 'ansible_python_interpreter=/usr/bin/python3' --tags acc_v8_pre_validations",
    execution_timeout=timedelta(hours=1),
    retries=1,
    retry_delay=timedelta(minutes=2),
    dag=dag,
)

task_pre_check_gather_cluster_data = PythonOperator(
    task_id="pre_check_gather_cluster_data_task",
    python_callable=pre_check_gather_cluster_data,
    op_kwargs={
        "instance_id": "{{ dag_run.conf['mkt'][0]['aciID'] }}",
    },
    provide_context=True,
    execution_timeout=timedelta(minutes=10),
    retries=1,
    retry_delay=timedelta(minutes=2),
    dag=dag,
)

task_pre_check_active_connections = PythonOperator(
    task_id="pre_check_active_connections_task",
    python_callable=pre_check_active_connections,
    op_kwargs={
        "instance_id": "{{ dag_run.conf['mkt'][0]['aciID'] }}",
    },
    provide_context=True,
    execution_timeout=timedelta(minutes=10),
    retries=1,
    retry_delay=timedelta(minutes=2),
    dag=dag,
)

task_pre_check_rt_mid_count = PythonOperator(
    task_id="pre_check_rt_mid_count_task",
    python_callable=pre_check_rt_mid_count,
    op_kwargs={
        "instance_id": "{{ dag_run.conf['mkt'][0]['aciID'] }}",
    },
    provide_context=True,
    execution_timeout=timedelta(minutes=10),
    retries=1,
    retry_delay=timedelta(minutes=2),
    dag=dag,
)

enable_newrelic_alert_muting = BashOperator(
    task_id="enable_newrelic_alert_muting",
    bash_command=f'''
        source {ansible_venv_path}/bin/activate && 
        source /opt/airflow/newrelic_creds.sh && 
        regular_app_servers=$(echo "{{{{ dag_run.conf["mkt"][0]["hosts"] }}}}" | tr "," "\\n" | grep -v -- "-x86$" | tr "\\n" "," | sed "s/,$//") && 
        x86_app_servers=$(echo "{{{{ dag_run.conf["mkt"][0]["hosts"] }}}}" | tr "," "\\n" | grep -- "-x86$" | tr "\\n" "," | sed "s/,$//") && 
        if [ ! -z "$regular_app_servers" ]; then 
            campaign-monitoring-controller --hosts "$regular_app_servers" --rule all --action mute --targets newrelic; 
        fi && 
        if [ ! -z "$x86_app_servers" ]; then 
            campaign-monitoring-controller --hosts "$x86_app_servers" --rule all --duration 1460 --action mute --targets newrelic; 
        fi
    ''',
    dag=dag,
    retries=0,
    trigger_rule="none_failed_min_one_success",
)

task_gather_and_set_data = PythonOperator(
    task_id="gather_and_set_data",
    python_callable=gather_and_set_data,
    op_kwargs={
        "instance_id": "{{ dag_run.conf['mkt'][0]['aciID'] }}",
    },
    provide_context=True,
    trigger_rule="none_failed_min_one_success",
    execution_timeout=timedelta(minutes=1),
    retries=1,
    dag=dag,
)


task_prechange_experience_audit_skip = BranchPythonOperator(
    task_id='prechange_experience_audit_skip_task',
    python_callable=skip_task,
    op_kwargs={
        'do_skip': skip_pre_experience_audit,
        'task_id_no_skip': [
            'prechange_experience_audit_task',
        ],
        'task_id_skip': [
            'execute_change_fork',
        ],
    },
    trigger_rule="none_failed_or_skipped",
    dag=dag,
)

task_prechange_experience_audit = UCOTriggerDagRunOperator(
    task_id="prechange_experience_audit_task",
    trigger_dag_id=experience_audit_dag_id,
    conf="{{ dag_run.conf[params.env][params.int_index] | getexperienceauditpayload(task_instance=ti, ignore_failure=True, audit_phase='prechange') }}",
    params={
        'env': 'mkt',
        'int_index': 0,
    },
    run_num = 1,
    parent_execution_date="{{ execution_date }}",
    reset_dag_run=True,
    wait_for_completion=True,
    failed_states=['failed'],
    dag=dag,
    retries=0,
)

execute_change_fork = DummyOperator(
    task_id="execute_change_fork",
    trigger_rule="none_failed_or_skipped",
    dag=dag,
)

# For future whenever you implement the mkt branch, ensure you add the field mktTenantID to the conf that is passed to the ucoyoda_acc_v8ARMCellReadiness DAG
mkt_execute_change = DummyOperator(
    task_id="mkt-execute_change_task",
    trigger_rule=TriggerRule.ALL_FAILED,
    dag=dag,
)


create_metadata = PythonOperator(
    task_id="create_metadata",
    python_callable=create_and_upload_metadata,
    provide_context=True,
    execution_timeout=timedelta(minutes=10),
    retries=2,
    retry_delay=timedelta(minutes=1),
    trigger_rule="none_failed_or_skipped",
    dag=dag,
)


cluster_adaptation = UCOTriggerDagRunOperator(
    task_id="cluster_adaptation",
    trigger_dag_id="ucoyoda_acc_charms_cluster_adaptations",
    conf="{{ {'cloudAccountId': dag_run.conf['mkt'][0]['cloudAccountId'] if dag_run.conf['mkt'] else dag_run.conf['cloudAccountId'], 'customerID': dag_run.conf['mkt'][0]['customerID'] if dag_run.conf['mkt'] else dag_run.conf['customerID'], 'mktTenantID': ti.xcom_pull(task_ids='gather_and_set_data', key='tenant_id')} }}",
    run_num=1,
    parent_execution_date="{{ execution_date }}",
    reset_dag_run=True,
    wait_for_completion=True,
    failed_states=["failed"],
    trigger_rule="none_failed_or_skipped",
    dag=dag,
    retries=0,
)

task_postchange_experience_audit_skip = BranchPythonOperator(
    task_id='postchange_experience_audit_skip_task',
    python_callable=skip_task,
    op_kwargs={
        'do_skip': skip_post_experience_audit,
        'task_id_no_skip': ['postchange_experience_audit_task'],
        'task_id_skip': ['task_complete_dag'],
    },
    dag=dag,
)

task_postchange_experience_audit = UCOTriggerDagRunOperator(
    task_id="postchange_experience_audit_task",
    trigger_dag_id=experience_audit_dag_id,
    conf="{{ dag_run.conf[params.env][params.int_index] | getexperienceauditpayload(task_instance=ti, ignore_failure=True, audit_phase='postchange') }}",
    params={
        'env': 'mkt',
        'int_index': 0,
    },
    run_num=2,  # Different run number to ensure new DAG run
    parent_execution_date="{{ execution_date }}",
    reset_dag_run=False,  # Don't reset to avoid conflicts with prechange run
    wait_for_completion=True,
    failed_states=["failed"],
    dag=dag,
    retries=0,
)

disable_newrelic_alert_muting = BashOperator(
    task_id="disable_newrelic_alert_muting",
    bash_command='source ' + ansible_venv_path + '/bin/activate && ' +
    'source /opt/airflow/newrelic_creds.sh && ' + 'campaign-monitoring-controller --hosts ' +
    app_servers + ' --rule all --action unmute --targets newrelic',
    dag=dag,
    retries=1,
)

task_post_service_check = BashOperator(
    task_id="post_service_check",
    bash_command = "source {provisioning_venv_path}/bin/activate && ansible-playbook -i {mid_rt_servers} {ansible_playbook_path}/post-validation-task.yml".format(
                provisioning_venv_path=provisioning_venv_path,
                mid_rt_servers=mid_rt_servers,
                ansible_playbook_path=ansible_playbook_path,
            ),
    execution_timeout=timedelta(hours=1),
    retries=1,
    retry_delay=timedelta(minutes=2),
    dag=dag,
)

task_complete_dag = DummyOperator(
    task_id="complete_dag",
    trigger_rule="none_failed_or_skipped",
    dag=dag
)

monitoring = SlackMonitorOperator(
    task_id="monitoring",
    channel="#charms-rollout-alerts-prod",
    conf_key="mkt",
    metadata=[
        "customerName",
        "instanceName",
        "customerID",
        "instanceUrl",
        "hosts",
        "region",
    ],
    update_interval=10,
    upload_failure_logs=True,
    notify_groups=["@charms-eng"],
    trigger_rule="none_failed_min_one_success",
    dag=dag,
)

""" Section 4: Task Definitions : END"""

""" Section 5: Task Dependencies : START"""
monitoring  # Runs independently, monitors other tasks
# Connect start task to pre-checks skip
task_start_run_dag >> task_pre_checks_skip

# Dependencies for pre-checks
task_pre_checks_skip >> task_pre_check_db
task_pre_checks_skip >> [task_pre_check_db_version, task_pre_schema_generation, task_pre_validations]
task_pre_checks_skip >> [task_pre_url_service_check, task_pre_infra_checks]
task_pre_checks_skip >> task_pre_check_gather_cluster_data
task_pre_checks_skip >> task_pre_check_active_connections
task_pre_checks_skip >> task_pre_check_rt_mid_count

[task_pre_check_db] >> enable_newrelic_alert_muting
[task_pre_check_db_version, task_pre_schema_generation, task_pre_validations] >> enable_newrelic_alert_muting
[task_pre_url_service_check, task_pre_infra_checks] >> enable_newrelic_alert_muting
[task_pre_check_gather_cluster_data] >> enable_newrelic_alert_muting
[task_pre_check_active_connections] >> enable_newrelic_alert_muting
[task_pre_check_rt_mid_count] >> enable_newrelic_alert_muting

enable_newrelic_alert_muting >> task_gather_and_set_data

task_gather_and_set_data >> task_prechange_experience_audit_skip >> task_prechange_experience_audit
task_prechange_experience_audit >> create_metadata
[task_prechange_experience_audit_skip, create_metadata] >> execute_change_fork
execute_change_fork >> mkt_execute_change

for env in environments:

    execute_change = TaskGroup(
        group_id=env + "-execute-change",
        dag=dag,
    )

    if env == "rt":
        # RT environment: Parallel execution without branch tasks
        for i in range(V8_MAX_SUPPORTED_PARALLELISM[env]):
            SkippableUCOTriggerDagRunOperator(
                task_id=env + "-execute-change-on-appserver-" + str(i),
                trigger_dag_id="ucoyoda_acc_v8ARMCellReadiness",
                conf="{{ dag_run.conf[params.env][params.int_index] | enrichdagconf(key='mktTenantID', value=ti.xcom_pull(task_ids='gather_and_set_data', key='tenant_id')) if dag_run.conf and params.env in dag_run.conf and params.int_index < (dag_run.conf[params.env]|length) else {} }}",
                run_num=1,
                params={
                    "env": env,
                    "int_index": i,
                },
                parent_execution_date="{{ execution_date }}",
                reset_dag_run=True,
                wait_for_completion=True,
                failed_states=["failed"],
                trigger_rule="none_failed_or_skipped",
                dag=dag,
                task_group=execute_change,
                retries=0,
            )

        # All RT tasks run in parallel - no sequential dependency
        execute_change_fork >> execute_change

    else:
        # MID environment: Simplified parallel execution (similar to RT)
        for i in range(V8_MAX_SUPPORTED_PARALLELISM[env]):
            # Single execute change task that calls ucoyoda_acc_v8ARMCellReadiness
            SkippableUCOTriggerDagRunOperator(
                task_id=env + "-execute-change-on-all-appserver-" + str(i),
                trigger_dag_id="ucoyoda_acc_v8ARMCellReadiness",
                conf="{{ dag_run.conf[params.env][params.int_index] | enrichdagconf(key='mktTenantID', value=ti.xcom_pull(task_ids='gather_and_set_data', key='tenant_id')) if dag_run.conf and params.env in dag_run.conf and params.int_index < (dag_run.conf[params.env]|length) else {} }}",
                run_num=1,
                params={
                    "env": env,
                    "int_index": i,
                },
                parent_execution_date="{{ execution_date }}",
                reset_dag_run=True,
                wait_for_completion=True,
                failed_states=["failed"],
                trigger_rule="none_failed_or_skipped",
                dag=dag,
                task_group=execute_change,
                retries=0,
            )

        # All MID tasks run in parallel
        execute_change_fork >> execute_change

    execute_change >> cluster_adaptation

mkt_execute_change >> cluster_adaptation
cluster_adaptation >> task_post_service_check
task_post_service_check >> task_postchange_experience_audit_skip

task_postchange_experience_audit_skip >> task_postchange_experience_audit
task_postchange_experience_audit_skip >> task_complete_dag
task_postchange_experience_audit >> disable_newrelic_alert_muting

disable_newrelic_alert_muting >> task_complete_dag

""" Section 5: Task Dependencies : END"""