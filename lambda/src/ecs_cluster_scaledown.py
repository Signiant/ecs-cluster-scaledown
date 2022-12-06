import logging
import os
import botocore
import boto3
import json
from operator import itemgetter

logging.getLogger('botocore').setLevel(logging.CRITICAL)
logging.getLogger('urllib3').setLevel(logging.CRITICAL)


def _get_instances_in_cluster(ecs, cluster_name, next_token=None, status=None):
    """Get instances in the given cluster"""
    result = []
    if next_token:
        if status:
            query_result = ecs.list_container_instances(cluster=cluster_name, nextToken=next_token, status=status)
        else:
            query_result = ecs.list_container_instances(cluster=cluster_name, nextToken=next_token)
    else:
        if status:
            query_result = ecs.list_container_instances(cluster=cluster_name, status=status)
        else:
            query_result = ecs.list_container_instances(cluster=cluster_name)
    if 'ResponseMetadata' in query_result:
        if 'HTTPStatusCode' in query_result['ResponseMetadata']:
            if query_result['ResponseMetadata']['HTTPStatusCode'] == 200:
                if 'nextToken' in query_result:
                    result.extend(query_result['containerInstanceArns'])
                    result.extend(_get_instances_in_cluster(ecs=ecs,
                                                            cluster_name=cluster_name,
                                                            next_token=query_result['nextToken'],
                                                            status=status))
                else:
                    result.extend(query_result['containerInstanceArns'])
    return result


def _get_instance_id(ecs, cluster_name, container_instance_id):
    query_result = ecs.describe_container_instances(cluster=cluster_name, containerInstances=[container_instance_id])
    instance_id = None
    if 'containerInstances' in query_result:
        instance_id = query_result['containerInstances'][0]['ec2InstanceId']
    return instance_id


def _get_instance_az(ec2, instance_id):
    query_result = ec2.describe_instances(InstanceIds=[instance_id])
    az = None
    if 'Reservations' in query_result:
        if 'Instances' in query_result['Reservations'][0]:
            az = query_result['Reservations'][0]['Instances'][0]['Placement']['AvailabilityZone']
    return az


def _get_container_instance_id(ecs, cluster_name, instance_id):
    query_result = ecs.list_container_instances(cluster=cluster_name)
    container_instance_id = None
    for ci in query_result['containerInstanceArns']:
        if _get_instance_id(ecs, cluster_name, ci) == instance_id:
            container_instance_id = ci
    return container_instance_id


def _get_autoscaling_group_name(ec2, instance_id):
    asg_name = None
    query_result = ec2.describe_instances(InstanceIds=[instance_id])
    if 'Reservations' in query_result and 'Instances' in query_result['Reservations'][0]:
        instance_tags = query_result['Reservations'][0]['Instances'][0]['Tags']
        try:
            asg_name = [d['Value'] for d in instance_tags if d['Key'] == 'aws:autoscaling:groupName'][0]
        except:
            pass
    return asg_name


def _get_autoscaling_group_min_size(asg, autoscaling_group_name):
    query_result = asg.describe_auto_scaling_groups(AutoScalingGroupNames=[autoscaling_group_name])
    if 'AutoScalingGroups' in query_result:
        return query_result['AutoScalingGroups'][0]['MinSize']
    else:
        return None


def _get_instance_task_count(ecs, cluster_name, container_instance_id):
    number_of_tasks = 0
    task_list_query_result = ecs.list_tasks(cluster=cluster_name, containerInstance=container_instance_id)
    if 'taskArns' in task_list_query_result:
        number_of_tasks = len(task_list_query_result['taskArns'])
    return number_of_tasks


def _get_sorted_instance_list_with_info(ecs, ec2, cluster_name):
    """ Return a list of instance objects in the cluster, ordered by number of tasks running on each """
    cluster_instance_list = _get_instances_in_cluster(ecs, cluster_name, status='ACTIVE')
    unsorted_instance_list = []
    for instance in cluster_instance_list:
        number_of_tasks = _get_instance_task_count(ecs, cluster_name, instance)
        instance_id = _get_instance_id(ecs, cluster_name, instance)
        instance_az = _get_instance_az(ec2, instance_id)
        item = {
            'container_instance_id': instance,
            'az': instance_az,
            'task_count': number_of_tasks
        }
        unsorted_instance_list.append(item)
    sorted_instance_list = sorted(unsorted_instance_list, key=itemgetter('task_count'))
    return sorted_instance_list


def _start_draining_instances(ecs, cluster_name, container_instance_id_list, dry_run=False):
    """ Put the given instance in a draining state """
    logging.debug(f"Attempting to put the following container instances in a DRAINING state: {str(container_instance_id_list)}")
    if not dry_run:
        try:
            action_result = ecs.update_container_instances_state(cluster=cluster_name,
                                                                 containerInstances=container_instance_id_list,
                                                                 status='DRAINING')
            if 'ResponseMetadata' in action_result:
                if 'HTTPStatusCode' in action_result['ResponseMetadata']:
                    if action_result['ResponseMetadata']['HTTPStatusCode'] != 200:
                        logging.error("Unexpected HTTPStatusCode - Unable to put instances in DRAINING state")
                        return False
                else:
                    logging.error("No HTTPStatusCode in response - Unable to put instances in DRAINING state")
                    return False
            else:
                logging.error("No ResponseMetaData in response - Unable to put instances in DRAINING state")
                return False
            # TODO: Check containerInstances returned and verify instances in question are in DRAINING state
            # If failures list is > 0, print out errors
            if len(action_result['failures']) > 0:
                for inst in action_result['failures']:
                    logging.error(f"Failure putting container instance into DRAINING state: {inst}")
                return False
            else:
                return True
        except botocore.exceptions.ClientError as e:
            logging.error(f'Unexpected error: {e}')
    else:
        logging.warning("   Dry run selected - will NOT put instances into DRAINING state")


def _get_instance_tasks(ecs, cluster_name, container_instance_id, next_token=None):
    """ Get a list of tasks running for the given instance """
    result = []
    if next_token:
        query_result = ecs.list_tasks(cluster=cluster_name, containerInstance=container_instance_id, nextToken=next_token)
    else:
        query_result = ecs.list_tasks(cluster=cluster_name, containerInstance=container_instance_id)
    if 'ResponseMetadata' in query_result:
        if 'HTTPStatusCode' in query_result['ResponseMetadata']:
            if query_result['ResponseMetadata']['HTTPStatusCode'] == 200:
                if 'nextToken' in query_result:
                    result.extend(query_result['taskArns'])
                    result.extend(_get_instance_tasks(ecs=ecs,
                                                      cluster_name=cluster_name,
                                                      container_instance_id=container_instance_id,
                                                      next_token=query_result['nextToken']))
                else:
                    result.extend(query_result['taskArns'])
    return result


def _can_be_terminated(ecs, cluster_name, container_instance_id, ignore_list=None):
    """
    Determine if the given instance can be terminated
    An instance is deemed ready for termination if no tasks are running on it, or
    only tasks matching the ignore_list are left running on it
    """
    if ignore_list is None:
        ignore_list = []
    task_count = _get_instance_task_count(ecs, cluster_name, container_instance_id)
    if task_count == 0:
        logging.debug("No tasks running on this instance - can be terminated")
        return True
    elif task_count <= len(ignore_list):
        # There are the same number of tasks as the length of the ignore list - check them
        logging.debug(f"{cluster_name}: Number of tasks running on this instance equals the length of the ignore list - check tasks to see if they match")
        task_list = _get_instance_tasks(ecs, cluster_name, container_instance_id)
        # Double check number of tasks
        if len(task_list) > len(ignore_list):
            # Too many tasks
            return False
        else:
            query_result = ecs.describe_tasks(cluster=cluster_name,
                                              tasks=task_list)
            running_tasks = query_result['tasks']
            for task in list(running_tasks):
                for ignore in ignore_list:
                    if ignore in task['group']:
                        logging.debug(f'   Found {ignore} task - ignoring')
                        running_tasks.remove(task)
                        break
            # running_tasks should be zero at this point if we can terminate this instance
            if len(running_tasks) == 0:
                logging.debug(f"{cluster_name}: All tasks running on this instance in ignore list - can be terminated")
                return True
    else:
        # too many tasks
        logging.warning(f"{cluster_name}: Too many tasks on this instance - can NOT be terminated")
        return False


def _terminate_and_remove_from_autoscaling_group(ecs, asg, cluster_name, container_instance_id, dry_run=False):
    """ Terminate the given instance and remove it from the autoscaling group while decrementing the desired count """
    result = None
    try:
        query_result = ecs.describe_container_instances(cluster=cluster_name, containerInstances=[container_instance_id])
        result = f'{cluster_name}: Scheduled termination result for container instance {container_instance_id}: '
        if 'containerInstances' in query_result:
            instance_id = query_result['containerInstances'][0]['ec2InstanceId']
            container_instance_state = query_result['containerInstances'][0]['status']
            logging.debug(f"{cluster_name}: Instance {instance_id} to be terminated - currently in {container_instance_state} state")
            if 'DRAINING' not in container_instance_state:
                logging.warning(f"{cluster_name}: Container Instance not in DRAINING state - unexpected, but continuing anyway")
            if not dry_run:
                activity_result = asg.terminate_instance_in_auto_scaling_group(InstanceId=instance_id,
                                                                               ShouldDecrementDesiredCapacity=True)
                result += f"{activity_result['Activity']['StatusCode']}"
            else:
                logging.warning("Dry run selected - no modifications will be done")
                result += f"Successfully terminated and removed {instance_id} - dry run"
    except botocore.exceptions.ClientError as e:
        result += f'Unexpected error: {e}'
        logging.error(f'Unexpected error: {e}')
    return result


def remove_container_instance_from_ecs_cluster(ecs, asg, cluster_name, container_instance_id, ignore_list=None, dry_run=False):
    if ignore_list is None:
        ignore_list = []
    logging.info(f"{cluster_name}: Attempting to remove container instance with ID {container_instance_id} from cluster")

    if not dry_run:
        # Make sure instance in question is in DRAINING state before continuing
        if container_instance_id not in _get_instances_in_cluster(ecs, cluster_name, status='DRAINING'):
            logging.error(f"{cluster_name}: Container Instance {container_instance_id} not in DRAINING state - aborting")
            return False

        if _can_be_terminated(ecs, cluster_name, container_instance_id, ignore_list):
            result = _terminate_and_remove_from_autoscaling_group(ecs, asg, cluster_name, container_instance_id, dry_run)
            logging.info(result)
            return True
        else:
            logging.info(f"{cluster_name}: Container Instance {container_instance_id} not ready to be terminated - will try again later")
            return False
    else:
        logging.warning("   Dry run selected - don't terminate and remove...")
        return True


def remove_instance_from_ecs_cluster_by_instance_id(ecs, asg, cluster_name, instance_id, ignore_list=None, dry_run=False):
    if ignore_list is None:
        ignore_list = []
    logging.info(f"{cluster_name}: Asked to remove instance with ID {instance_id} from cluster")
    container_instance_id = _get_container_instance_id(ecs, cluster_name, instance_id)
    return remove_container_instance_from_ecs_cluster(ecs=ecs,
                                                      asg=asg,
                                                      cluster_name=cluster_name,
                                                      container_instance_id=container_instance_id,
                                                      ignore_list=ignore_list,
                                                      dry_run=dry_run)


def scale_down_ecs_cluster(ecs, ec2, asg, decrease_count, cluster_name=None, ignore_list=None, dry_run=False):
    """
    Scale down the given ECS cluster by the count given
    :param ecs: boto3 ECS client
    :param ec2: boto3 EC2 client
    :param asg: boto3 ASG client
    :param decrease_count: number of instances to remove from cluster
    :param cluster_name: name of the cluster to scale down
    :param ignore_list: list of tasks to ignore
    :param dry_run: dry run only - no changes
    :return: Boolean Success
    """
    if ignore_list is None:
        ignore_list = []
    if not cluster_name:
        logging.critical("Must provide cluster name")
    logging.info(f"{cluster_name}: Asked to scale down cluster by a count of {str(decrease_count)}")
    # Get an ordered list of instances in the cluster
    ordered_instances = _get_sorted_instance_list_with_info(ecs=ecs, ec2=ec2, cluster_name=cluster_name)
    container_instance_list = []
    for instance in ordered_instances:
        container_instance_list.append(instance['container_instance_id'])
    logging.debug(f"{cluster_name}: Cluster instance list: {json.dumps(ordered_instances)}")
    instance_count = len(container_instance_list)
    if instance_count <= 0:
        logging.error(f"{cluster_name}: No instances in cluster! Aborting")
        return False

    # Query an instance in the cluster for the Autoscaling Group Name
    instance_to_query = _get_instance_id(ecs, cluster_name, container_instance_list[0])
    asg_name = _get_autoscaling_group_name(ec2, instance_to_query)
    if asg_name:
        min_cluster_size = int(_get_autoscaling_group_min_size(asg, asg_name))
        logging.info(f"{cluster_name}: Determined minimum cluster size to be {str(min_cluster_size)}")
    else:
        logging.warning(f"{cluster_name}: Unable to determine minimum cluster size, defaulting to 1")
        min_cluster_size = 1

    if instance_count <= min_cluster_size:
        logging.error(f"{cluster_name}: Cluster is already at or below minimum size - aborting")
        return False

    if instance_count - decrease_count < min_cluster_size:
        # need to recalculate decrease_count
        logging.warning(f"{cluster_name}: Decreasing cluster by the given count, {decrease_count}, would result in cluster dropping below minimum size")
        decrease_count = instance_count - min_cluster_size
        logging.warning(f"{cluster_name}: Cluster min size is {min_cluster_size}, current size is {instance_count}, can decrease by a maximum of {decrease_count}")

    if decrease_count <= 0:
        logging.error(f"{cluster_name}: Not enough instances in cluster to reduce size")
        return False

    logging.info(f"{cluster_name}: Current cluster size: {instance_count}")

    # Determine number of instances in each az
    azs = {}
    for instance in ordered_instances:
        az_name = instance['az']
        if az_name not in azs:
            azs[az_name] = []
        azs[az_name].append(instance['container_instance_id'])

    logging.debug(f"AZ dict: {json.dumps(azs)}")

    for az in azs:
        logging.info(f"   Count in {az}: {len(azs[az])}")

    terminate_list = []
    # Only handle 2 AZs for now
    if len(azs) == 1:
        # only one availability zone in use - just remove the top entry
        logging.debug(f'{cluster_name}: Only one availability zone in play - select the least loaded instance')
        terminate_list = container_instance_list[:decrease_count]
    elif len(azs) == 2:
        az_names = []
        for az in azs:
            az_names.append(az)
        for x in range(0, decrease_count):
            # two availability zones in use
            if len(azs[az_names[0]]) > len(azs[az_names[1]]) or len(azs[az_names[0]]) == len(azs[az_names[1]]):
                # first group is greater than or equal to the second group - first take from group 1
                instance_to_terminate = azs[az_names[0]].pop(0)
                logging.debug(f'{cluster_name}: Selecting instance from AZ: {az_names[0]}')
                terminate_list.append(instance_to_terminate)
            else:
                # first group is smaller than the second group - first take from group 2
                instance_to_terminate = azs[az_names[1]].pop(0)
                logging.debug(f'{cluster_name}:` Selecting instance from AZ: {az_names[1]}')
                terminate_list.append(instance_to_terminate)
    else:
        logging.error(f"{cluster_name}: Can't handle more than 2 availability zones currently")
        return

    logging.debug(f"{cluster_name}: Terminate instance list: {terminate_list}")
    # Drain the least loaded instances
    _start_draining_instances(ecs, cluster_name, terminate_list, dry_run)

    for inst in terminate_list[:]:
        remove_container_instance_from_ecs_cluster(ecs=ecs,
                                                   asg=asg,
                                                   cluster_name=cluster_name,
                                                   container_instance_id=inst,
                                                   ignore_list=ignore_list,
                                                   dry_run=dry_run)


def lambda_handler(event, context):
    log_level = os.environ.get('LOG_LEVEL', 'INFO')
    if 'debug' in log_level.lower():
        logging_level = logging.DEBUG
    else:
        logging_level = logging.INFO
    logging.getLogger().setLevel(logging_level)

    logging.debug(f'Event: {event}')
    cluster_name = event.get('cluster_name', None)
    count = event.get('count', 1)
    instance_ids = event.get('instance_ids', None)
    ignore_list = event.get('ignore_list', None)
    alarm_name = event.get('alarm_name', None)
    region = event.get('region', None)
    dry_run = event.get('dry_run', False)

    if not cluster_name or not region:
        logging.critical("Must provide cluster name and region - aborting")
        return

    session = boto3.session.Session(region_name=region)
    ecs_client = session.client('ecs')
    ec2_client = session.client('ec2')
    asg_client = session.client('autoscaling')

    logging.info(f'Starting Scale Down Process for cluster: {cluster_name}')

    # First check for instances in DRAINING state and remove them from the cluster if possible
    logging.info(f'{cluster_name}: Checking for any instances in DRAINING state')
    draining_instances = _get_instances_in_cluster(ecs_client, cluster_name, status='DRAINING')
    if len(draining_instances) > 0:
        logging.info(f'{cluster_name}: found {len(draining_instances)} instances in DRAINING state - removing')
        for instance in draining_instances:
            remove_container_instance_from_ecs_cluster(ecs=ecs_client,
                                                       asg=asg_client,
                                                       cluster_name=cluster_name,
                                                       container_instance_id=instance,
                                                       ignore_list=ignore_list,
                                                       dry_run=dry_run)
    else:
        logging.info(f'{cluster_name}: no instances found in DRAINING state')

    # providing a count of 0 will simply result in terminating instances that
    # are in a DRAINING state and not trying to scale down any further
    if count > 0:
        if alarm_name:
            cw_client = session.client('cloudwatch')
            logging.debug(f'Querying for alarm with name {alarm_name} in ALARM state in the {region} region')
            query_result = cw_client.describe_alarms(AlarmNames=[alarm_name], StateValue='ALARM')
            # logging.debug(str(query_result))
            matching_alarms = query_result.get('MetricAlarms')
            logging.debug(f'Found {len(matching_alarms)} alarms in ALARM state')
            if len(matching_alarms) == 0:
                logging.warning(f"Given alarm ({alarm_name}) NOT in alarm state - NOT attempting scale down of cluster")
                return

        if instance_ids:
            for instance in instance_ids:
                remove_instance_from_ecs_cluster_by_instance_id(ecs=ecs_client,
                                                                asg=asg_client,
                                                                cluster_name=cluster_name,
                                                                instance_id=instance,
                                                                ignore_list=ignore_list,
                                                                dry_run=dry_run)
        else:
            scale_down_ecs_cluster(ecs=ecs_client,
                                   ec2=ec2_client,
                                   asg=asg_client,
                                   decrease_count=count,
                                   cluster_name=cluster_name,
                                   ignore_list=ignore_list,
                                   dry_run=dry_run)
