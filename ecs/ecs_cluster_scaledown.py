import logging.handlers
import argparse
import botocore
import boto3
import json
from operator import itemgetter
import sys

logging.getLogger('botocore').setLevel(logging.CRITICAL)


def _get_instances_in_cluster(cluster_name, next_token=None, status=None):
    """Get instances in the given cluster"""
    result = []
    if next_token:
        if status:
            query_result = ECS.list_container_instances(cluster=cluster_name, nextToken=next_token, status=status)
        else:
            query_result = ECS.list_container_instances(cluster=cluster_name, nextToken=next_token)
    else:
        if status:
            query_result = ECS.list_container_instances(cluster=cluster_name, status=status)
        else:
            query_result = ECS.list_container_instances(cluster=cluster_name)
    if 'ResponseMetadata' in query_result:
        if 'HTTPStatusCode' in query_result['ResponseMetadata']:
            if query_result['ResponseMetadata']['HTTPStatusCode'] == 200:
                if 'nextToken' in query_result:
                    result.extend(query_result['containerInstanceArns'])
                    result.extend(_get_instances_in_cluster(cluster_name=cluster_name,
                                                            next_token=query_result['nextToken'],
                                                            status=status))
                else:
                    result.extend(query_result['containerInstanceArns'])
    return result


def _get_instance_id(cluster_name, container_instance_id):
    query_result = ECS.describe_container_instances(cluster=cluster_name, containerInstances=[container_instance_id])
    instance_id = None
    if 'containerInstances' in query_result:
        instance_id = query_result['containerInstances'][0]['ec2InstanceId']
    return instance_id


def _get_instance_az(instance_id):
    query_result = EC2.describe_instances(InstanceIds=[instance_id])
    az = None
    if 'Reservations' in query_result:
        if 'Instances' in query_result['Reservations'][0]:
            az = query_result['Reservations'][0]['Instances'][0]['Placement']['AvailabilityZone']
    return az


def _get_container_instance_id(cluster_name, instance_id):
    query_result = ECS.list_container_instances(cluster=cluster_name)
    container_instance_id = None
    for ci in query_result['containerInstanceArns']:
        if _get_instance_id(cluster_name, ci) == instance_id:
            container_instance_id = ci
    return container_instance_id


def _get_autoscaling_group_name(instance_id):
    EC2 = SESSION.client('ec2')
    asg_name = None
    query_result = EC2.describe_instances(InstanceIds=[instance_id])
    if 'Reservations' in query_result and 'Instances' in query_result['Reservations'][0]:
        instance_tags = query_result['Reservations'][0]['Instances'][0]['Tags']
        try:
            asg_name = [d['Value'] for d in instance_tags if d['Key'] == 'aws:autoscaling:groupName'][0]
        except:
            pass
    return asg_name


def _get_autoscaling_group_min_size(autoscaling_group_name):
    query_result = ASG.describe_auto_scaling_groups(AutoScalingGroupNames=[autoscaling_group_name])
    if 'AutoScalingGroups' in query_result:
        return query_result['AutoScalingGroups'][0]['MinSize']
    else:
        return None


def _get_instance_task_count(cluster_name, container_instance_id):
    number_of_tasks = 0
    task_list_query_result = ECS.list_tasks(cluster=cluster_name, containerInstance=container_instance_id)
    if 'taskArns' in task_list_query_result:
        number_of_tasks = len(task_list_query_result['taskArns'])
    return number_of_tasks


def _get_sorted_instance_list_with_info(cluster_name):
    ''' Return a list of instance objects in the cluster, ordered by number of tasks running on each '''
    cluster_instance_list = _get_instances_in_cluster(cluster_name, status='ACTIVE')
    unsorted_instance_list = []
    for instance in cluster_instance_list:
        number_of_tasks = _get_instance_task_count(cluster_name, instance)
        instance_id = _get_instance_id(cluster_name, instance)
        instance_az = _get_instance_az(instance_id)
        item = {
            'container_instance_id': instance,
            'az': instance_az,
            'task_count': number_of_tasks
        }
        unsorted_instance_list.append(item)
    sorted_instance_list = sorted(unsorted_instance_list, key=itemgetter('task_count'))
    return sorted_instance_list


def _start_draining_instances(cluster_name, container_instance_id_list, dryrun=False):
    """ Put the given instance in a draining state """
    logging.debug("Attempting to put the following container instances in a DRAINING state: %s" % str(container_instance_id_list))
    if not dryrun:
        try:
            action_result = ECS.update_container_instances_state(cluster=cluster_name,
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
                    logging.error("Failure putting container instance into DRAINING state: %s" % inst)
                return False
            else:
                return True
        except botocore.exceptions.ClientError as e:
            logging.error('Unexpected error: %s' % e)
    else:
        logging.warning("   Dryrun selected - will NOT put instances into DRAINING state")


def _get_instance_tasks(cluster_name, container_instance_id, next_token=None):
    """ Get a list of tasks running for the given instance """
    result = []
    if next_token:
        query_result = ECS.list_tasks(cluster=cluster_name, containerInstance=container_instance_id, nextToken=next_token)
    else:
        query_result = ECS.list_tasks(cluster=cluster_name, containerInstance=container_instance_id)
    if 'ResponseMetadata' in query_result:
        if 'HTTPStatusCode' in query_result['ResponseMetadata']:
            if query_result['ResponseMetadata']['HTTPStatusCode'] == 200:
                if 'nextToken' in query_result:
                    result.extend(query_result['taskArns'])
                    result.extend(_get_instance_tasks(cluster_name=cluster_name,
                                                      containerInstance=container_instance_id,
                                                      next_token=query_result['nextToken']))
                else:
                    result.extend(query_result['taskArns'])
    return result


def _can_be_terminated(cluster_name, container_instance_id, ignore_list=[]):
    """
    Determine if the given instance can be terminated
    An instance is deemed ready for termination if no tasks are running on it, or
    only tasks matching the ignore_list are left running on it
    """
    task_count = _get_instance_task_count(cluster_name, container_instance_id)
    if task_count == 0:
        logging.debug("No tasks running on this instance - can be terminated")
        return True
    elif task_count <= len(ignore_list):
        # There are the same number of tasks as the length of the ignore list - check them
        logging.debug("%s: Number of tasks running on this instance equals the length of the ignore list - check tasks to see if they match" % cluster_name)
        task_list = _get_instance_tasks(cluster_name, container_instance_id)
        # Double check number of tasks
        if len(task_list) > len(ignore_list):
            # Too many tasks
            return False
        else:
            query_result = ECS.describe_tasks(cluster=cluster_name,
                                              tasks=task_list)
            running_tasks = query_result['tasks']
            for task in list(running_tasks):
                for ignore in ignore_list:
                    if ignore in task['group']:
                        logging.debug('   Found %s task - ignoring' % ignore)
                        running_tasks.remove(task)
                        break
            # running_tasks should be zero at this point if we can terminate this instance
            if len(running_tasks) == 0:
                logging.debug("%s: All tasks running on this instance in ignore list - can be terminated" % cluster_name)
                return True
    else:
        # too many tasks
        logging.warning("%s: Too many tasks on this instance - can NOT be terminated" % cluster_name)
        return False


def _terminate_and_remove_from_autoscaling_group(cluster_name, container_instance_id, dryrun=False):
    """ Terminate the given instance and remove it from the autoscaling group while decrementing the desired count """
    result = None
    try:
        query_result = ECS.describe_container_instances(cluster=cluster_name, containerInstances=[container_instance_id])
        result = '%s: Scheduled termination result for container instance %s: ' % (cluster_name, container_instance_id)
        if 'containerInstances' in query_result:
            instance_id = query_result['containerInstances'][0]['ec2InstanceId']
            container_instance_state = query_result['containerInstances'][0]['status']
            logging.debug("%s: Instance %s to be terminated - currently in %s state" % (cluster_name, instance_id, container_instance_state))
            if not 'DRAINING' in container_instance_state:
                logging.warning("%s: Container Instance not in DRAINING state - unexpected, but continuing anyway" % cluster_name)
            if not dryrun:
                activity_result = ASG.terminate_instance_in_auto_scaling_group(InstanceId=instance_id,
                                                                               ShouldDecrementDesiredCapacity=True)
                result += "%s" % activity_result['Activity']['StatusCode']
            else:
                logging.warning("Dryrun selected - no modifications will be done")
                result += "Successfully terminated and removed %s - dryrun" % instance_id
    except botocore.exceptions.ClientError as e:
        result += 'Unexpected error: %s' % e
        logging.error('Unexpected error: %s' % e)
    return result


def remove_container_instance_from_ecs_cluster(cluster_name, container_instance_id, ignore_list=[], dryrun=False):
    logging.info("%s: Attempting to remove container instance with ID %s from cluster" % (cluster_name, container_instance_id))

    if not dryrun:
        # Make sure instance in question is in DRAINING state before continuing
        if not container_instance_id in _get_instances_in_cluster(cluster_name, status='DRAINING'):
            logging.error("%s: Container Instance %s not in DRAINING state - aborting" % (cluster_name, container_instance_id))
            return False

        if _can_be_terminated(cluster_name, container_instance_id, ignore_list):
            result = _terminate_and_remove_from_autoscaling_group(cluster_name, container_instance_id, dryrun)
            logging.info(result)
            return True
        else:
            logging.info("%s: Container Instance %s not ready to be terminated - will try again later" % (cluster_name, container_instance_id))
            return False
    else:
        logging.warning("   Dryrun selected - don't terminate and remove...")
        return True


def remove_instance_from_ecs_cluster_by_instance_id(cluster_name, instance_id, ignore_list=[], dryrun=False):
    logging.info("%s: Asked to remove instance with ID %s from cluster" % (cluster_name, instance_id))
    container_instance_id = _get_container_instance_id(cluster_name, instance_id)
    return remove_container_instance_from_ecs_cluster(cluster_name=cluster_name,
                                                      container_instance_id=container_instance_id,
                                                      ignore_list=ignore_list,
                                                      dryrun=dryrun)


def scale_down_ecs_cluster(decrease_count, cluster_name=None, ignore_list=[], dryrun=False):
    """
    Scale down the given ECS cluster by the count given
    :param decrease_count: number of instances to remove from cluster
    :param cluster_name: name of the cluster to scale down
    :param ignore_list: list of tasks to ignore
    :param dryrun: dryrun only - no changes
    :return: Boolean Success
    """
    if not cluster_name:
        logging.critical("Must provide cluster name")
    logging.info("%s: Asked to scale down cluster by a count of %s" % (cluster_name, str(decrease_count)))
    # Get an ordered list of instances in the cluster
    ordered_instances = _get_sorted_instance_list_with_info(cluster_name=cluster_name)
    container_instance_list = []
    for instance in ordered_instances:
        container_instance_list.append(instance['container_instance_id'])
    logging.debug("%s: Cluster instance list:\n%s" % (cluster_name, json.dumps(ordered_instances, indent=4)))
    instance_count = len(container_instance_list)
    if instance_count <= 0:
        logging.error("%s: No instances in cluster! Aborting" % cluster_name)
        return False

    # Query an instance in the cluster for the Autoscaling Group Name
    instance_to_query = _get_instance_id(cluster_name, container_instance_list[0])
    asg_name = _get_autoscaling_group_name(instance_to_query)
    if asg_name:
        min_cluster_size = int(_get_autoscaling_group_min_size(asg_name))
        logging.info("%s: Determined minimum cluster size to be %s" % (cluster_name, str(min_cluster_size)))
    else:
        logging.warning("%s: Unable to determine minimum cluster size, defaulting to 1" % cluster_name)
        min_cluster_size = 1

    if instance_count <= min_cluster_size:
        logging.error("%s: Cluster is already at or below minimum size - unable to scale down further - aborting" % cluster_name)
        return False

    if instance_count - decrease_count < min_cluster_size:
        # need to recalculate decrease_count
        logging.warning("%s: Decreasing cluster by the given count, %s, would result in cluster dropping below minimum size" % (cluster_name, str(decrease_count)))
        decrease_count = instance_count - min_cluster_size
        logging.warning("%s: Cluster min size is %s, current size is %s, can decrease by a maximum of %s" % (cluster_name, min_cluster_size, instance_count, decrease_count))

    if decrease_count <= 0:
        logging.error("%s: Not enough instances in cluster to reduce size" % cluster_name)
        return False

    logging.info("%s: Current cluster size: %s" % (cluster_name, str(instance_count)))

    # Determine number of instances in each az
    azs = {}
    for instance in ordered_instances:
        az_name = instance['az']
        if not az_name in azs:
            azs[az_name] = []
        azs[az_name].append(instance['container_instance_id'])

    logging.debug("AZ dict:\n%s" % json.dumps(azs, indent=4))

    for az in azs:
        logging.info("   Count in %s: %s" % (az, len(azs[az])))

    terminate_list = []
    # Only handle 2 AZs for now
    if len(azs) == 1:
        # only one availability zone in use - just remove the top entry
        logging.debug('%s: Only one availability zone in play - select the least loaded instance' % cluster_name)
        terminate_list = container_instance_list[:decrease_count]
    elif len(azs) == 2:
        az_names = []
        for az in azs:
            az_names.append(az)
        for x in range(0,decrease_count):
            # two availability zones in use
            if len(azs[az_names[0]]) > len(azs[az_names[1]]) or len(azs[az_names[0]]) == len(azs[az_names[1]]):
                # first group is greater than or equal to the second group - first take from group 1
                instance_to_terminate = azs[az_names[0]].pop(0)
                logging.debug('%s: Selecting instance from AZ: %s' % (cluster_name, az_names[0]))
                terminate_list.append(instance_to_terminate)
            else:
                # first group is smaller than the second group - first take from group 2
                instance_to_terminate = azs[az_names[1]].pop(0)
                logging.debug('%s: Selecting instance from AZ: %s' % (cluster_name, az_names[1]))
                terminate_list.append(instance_to_terminate)
    else:
        logging.error("%s: Can't handle more than 2 availability zones currently" % cluster_name)
        sys.exit(1)

    logging.debug("%s: Terminate instance list: %s" % (cluster_name, str(terminate_list)))
    # Drain the least loaded instances
    _start_draining_instances(cluster_name, terminate_list, dryrun)

    for inst in terminate_list[:]:
        remove_container_instance_from_ecs_cluster(cluster_name=args.cluster_name,
                                                   container_instance_id=inst,
                                                   ignore_list=ignore_list,
                                                   dryrun=args.dryrun)


if __name__ == "__main__":

    LOG_FILENAME = 'ecs_cluster_scaledown.log'

    parser = argparse.ArgumentParser(description='ecs_cluster_scaledown')

    parser.add_argument("--aws-access-key-id", help="AWS Access Key ID", dest='aws_access_key', required=False)
    parser.add_argument("--aws-secret-access-key", help="AWS Secret Access Key", dest='aws_secret_key', required=False)
    parser.add_argument("--cluster-name", help="Cluster name", dest='cluster_name', required=True)
    parser.add_argument("--count", help="Number of instances to remove [1]", dest='count', type=int, default=1, required=False)
    parser.add_argument("--instance-ids", help="Instance ID(s) to be removed", dest='instance_ids', nargs='+', required=False)
    parser.add_argument("--ignore-list", help="Tasks to be ignored when determining running tasks", dest='ignore_list', nargs='+',required=False)
    parser.add_argument("--alarm-name", help="Alarm name to check if scale down should be attempted", dest='alarm_name', required=False)
    parser.add_argument("--region", help="The AWS region the cluster is in", dest='region', required=True)
    parser.add_argument("--profile", help="The name of an aws cli profile to use.", dest='profile', default=None, required=False)
    parser.add_argument("--verbose", help="Turn on DEBUG logging", action='store_true', required=False)
    parser.add_argument("--dryrun", help="Do a dryrun - no changes will be performed", dest='dryrun',
                        action='store_true', default=False, required=False)
    args = parser.parse_args()

    log_level = logging.INFO

    if args.verbose:
        print("Verbose logging selected")
        log_level = logging.DEBUG

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    # create file handler which logs even debug messages
    fh = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=5242880, backupCount=5)
    fh.setLevel(logging.DEBUG)
    # create console handler using level set in log_level
    ch = logging.StreamHandler(stream=sys.stdout)
    ch.setLevel(log_level)
    console_formatter = logging.Formatter('%(levelname)8s: %(message)s')
    ch.setFormatter(console_formatter)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)8s: %(message)s')
    fh.setFormatter(file_formatter)
    # Add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)

    SESSION = boto3.session.Session(profile_name=args.profile, region_name=args.region)
    ECS = SESSION.client('ecs')
    EC2 = SESSION.client('ec2')
    ASG = SESSION.client('autoscaling')

    logging.info('Starting Scale Down Process for cluster: %s' % args.cluster_name)

    # Check for instances in DRAINING state and remove them from the cluster if possible
    logging.info('%s: Checking for any instances in DRAINING state - if found will attempt to remove them from the cluster' % args.cluster_name)
    draining_instances = _get_instances_in_cluster(args.cluster_name, status='DRAINING')
    for instance in draining_instances:
        remove_container_instance_from_ecs_cluster(cluster_name=args.cluster_name,
                                                   container_instance_id=instance,
                                                   ignore_list=args.ignore_list,
                                                   dryrun=args.dryrun)

    # providing a count of 0 will simply result in terminating instances is a DRAINING state and not trying to scale down any further
    if args.count > 0:
        if args.alarm_name:
            cw = SESSION.client('cloudwatch')
            logging.debug('Querying for alarm with name %s in ALARM state in the %s region' % (args.alarm_name, args.region))
            query_result = cw.describe_alarms(AlarmNames=[args.alarm_name], StateValue='ALARM')
            # logging.debug(str(query_result))
            matching_alarms = query_result['MetricAlarms']
            logging.debug('Found %s alarms in ALARM state' % str(len(matching_alarms)))
            if len(matching_alarms) == 0:
                logging.warning("Given alarm (%s) is NOT in alarm state - will NOT attempt to scale down cluster" % args.alarm_name)
                sys.exit(0)

        if args.instance_ids:
            for instance in args.instance_ids:
                remove_instance_from_ecs_cluster_by_instance_id(cluster_name=args.cluster_name,
                                                                instance_id=instance,
                                                                ignore_list=args.ignore_list,
                                                                dryrun=args.dryrun)
        else:
            scale_down_ecs_cluster(decrease_count=args.count,
                                   cluster_name=args.cluster_name,
                                   ignore_list=args.ignore_list,
                                   dryrun=args.dryrun)
