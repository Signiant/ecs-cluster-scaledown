import logging.handlers
import argparse
import boto3
from operator import itemgetter
import datetime
import pytz
import time
import sys

logging.getLogger('botocore').setLevel(logging.CRITICAL)


def _get_instances_in_cluster(cluster_name, next_token=None, status=None):
    '''Get instances in the given cluster'''
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


def _get_instances_in_least_loaded_order(cluster_name):
    ''' Return a list of instances in the cluster, ordered by number of tasks running on each '''
    return_instance_list = []
    cluster_instance_list = _get_instances_in_cluster(cluster_name)
    unsorted_instance_list = []
    for instance in cluster_instance_list:
        number_of_tasks = _get_instance_task_count(cluster_name, instance)
        item = {
            'container_instance_id' : instance,
            'task_count': number_of_tasks
        }
        unsorted_instance_list.append(item)
    sorted_instance_list = sorted(unsorted_instance_list, key=itemgetter('task_count'))
    for instance in sorted_instance_list:
        return_instance_list.append(instance['container_instance_id'])
    return return_instance_list


def _start_draining_instances(cluster_name, container_instance_id_list, dryrun=False):
    ''' Put the given instance in a draining state '''
    logging.debug("Attempting to put the following container instances in a DRAINING state: %s" % str(container_instance_id_list))
    if not dryrun:
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
    else:
        logging.warning("Dryrun selected - will NOT put instances into DRAINING state")


def _get_instance_tasks(cluster_name, container_instance_id, next_token=None):
    ''' Get a list of tasks running for the given instance '''
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


def _can_be_terminated(cluster_name, container_instance_id):
    '''
    Determine if the given instance can be terminated
    An instance is deemed ready for termination if no tasks are running on it, or
    only the logspout task is left running on it
    '''
    task_count = _get_instance_task_count(cluster_name, container_instance_id)
    if task_count == 0:
        logging.debug("No tasks running on this instance - can be terminated")
        return True
    elif task_count == 1 :
        # There is one task running on this instance - see if it's logspout
        logging.debug("One tasks running on this instance - check if it's logspout")
        task_list = _get_instance_tasks(cluster_name, container_instance_id)
        # Double check number of tasks
        if len(task_list) > 1:
            return False
        else:
            query_result = ECS.describe_tasks(cluster=cluster_name,
                                              tasks=task_list)
            if 'LogspoutTask' in query_result['tasks'][0]['group']:
                logging.debug("One task on this instance IS logspout - can be terminated")
                # It's logspout - we're good
                return True
    else:
        # More than 1 task - no good
        logging.debug("More than one task on this instance - can NOT be terminated")
        return False


def _terminate_and_remove_from_autoscaling_group(cluster_name, container_instance_id, dryrun=False):
    ''' Terminate the given instance and remove it from the autoscaling group while decrementing the desired count '''
    result = 'Scheduled termination result for instance '
    query_result = ECS.describe_container_instances(cluster=cluster_name, containerInstances=[container_instance_id])
    if 'containerInstances' in query_result:
        instance_id = query_result['containerInstances'][0]['ec2InstanceId']
        container_instance_state = query_result['containerInstances'][0]['status']
        logging.debug("Instance %s to be terminated - currently in %s state" % (instance_id, container_instance_state))
        if not 'DRAINING' in container_instance_state:
            logging.warning("Container Instance not in DRAINING state - unexpected, but continuing anyway")
        result += '%s: ' % instance_id
        if not dryrun:
            activity_result = ASG.terminate_instance_in_auto_scaling_group(InstanceId=instance_id,
                                                                           ShouldDecrementDesiredCapacity=True)
            result += "%s" % activity_result['Activity']['StatusCode']
        else:
            logging.warning("Dryrun selected - no modifications will be done")
            result += "Successful dryrun"
    return result


def remove_instance_from_ecs_cluster(cluster_name, instance_id, dryrun=False):
    # Check to make sure there are no instances in the cluster that are in a DRAINING state before starting
    if len(_get_instances_in_cluster(cluster_name, status='DRAINING')) > 0:
        logging.error("Cluster contains instance(s) in DRAINING state - aborting")
        return False

    logging.info("Asked to remove instance with ID %s from cluster" % instance_id)

    container_instance_id = _get_container_instance_id(cluster_name, instance_id)
    logging.debug("Instance's container instance ARN is: %s" % container_instance_id)
    _start_draining_instances(cluster_name, [container_instance_id], dryrun)

    logging.info("Logging start time...")
    timer_start_utc = datetime.datetime.now(pytz.UTC)
    complete = False
    while not complete:
        if _can_be_terminated(cluster_name, container_instance_id):
            result = _terminate_and_remove_from_autoscaling_group(cluster_name, container_instance_id, dryrun)
            logging.info(result)
            complete = True
        if MAX_WAIT > 0:
            running_time = time_now_utc = datetime.datetime.now(pytz.UTC) - timer_start_utc
            running_time_seconds = running_time.total_seconds()
            running_time_hours = int(running_time_seconds // 3600)
            if running_time_hours > MAX_WAIT:
                logging.warning("MAX-WAIT time hit - terminating remaining instances")
                result = _terminate_and_remove_from_autoscaling_group(cluster_name, container_instance_id, dryrun)
                logging.info(result)
                complete = True
        if not complete:
            logging.debug("Sleeping for 60 seconds")
            time.sleep(60)
    return True


def scale_down_ecs_cluster(decrease_count, cluster_name=None, dryrun=False):
    '''
    Scale down the given ECS cluster by the count given
    :param decrease_count: number of instances to remove from cluster
    :param cluster_name: name of the cluster to scale down
    :param dryrun: dryrun only - no changes
    :return: Boolean Success
    '''
    if not cluster_name:
        logging.critical("Must provide cluster name")
    # Check to make sure there are no instances in the cluster that are in a DRAINING state before starting
    if len(_get_instances_in_cluster(cluster_name, status='DRAINING')) > 0:
        logging.error("Cluster contains instance(s) in DRAINING state - aborting")
        return False
    logging.info("Asked to scale down cluster by a count of %s" % str(decrease_count))
    # Get an ordered list of instances in the cluster
    instance_list = _get_instances_in_least_loaded_order(cluster_name=cluster_name)
    instance_count = len(instance_list)
    if instance_count <= 0:
        logging.error("No instances in cluster! Aborting")
        return False

    logging.info("Current cluster size: %s" % str(instance_count))

    logging.debug("Cluster instance list: %s" % str(instance_list))

    # Query an instance in the cluster for the Autoscaling Group Name
    instance_to_query = _get_instance_id(cluster_name, instance_list[0])
    asg_name = _get_autoscaling_group_name(instance_to_query)
    if asg_name:
        min_cluster_size = int(_get_autoscaling_group_min_size(asg_name))
        logging.info("Determined minimum cluster size to be %s" % str(min_cluster_size))
    else:
        logging.warning("Unable to determine minimum cluster size, defaulting to 1")
        min_cluster_size = 1

    logging.debug("Cluster instance count: %s" % str(instance_count))

    if instance_count <= min_cluster_size:
        logging.error("Cluster is already at or below minimum size - unable to scale down further - aborting")
        return False

    if instance_count - decrease_count < min_cluster_size:
        # need to recalculate decrease_count
        logging.warn("Decreaseing cluster by the given count, %s, would result in cluster dropping below minimum size" % str(decrease_count))
        decrease_count = instance_count - min_cluster_size
        logging.warn("Cluster min size is %s, current size is %s, can decrease by a maximum of %s" % (min_cluster_size, instance_count, decrease_count))

    # Drain the least loaded instances
    if decrease_count > 0:
        terminate_list = instance_list[:decrease_count]
        _start_draining_instances(cluster_name, terminate_list, dryrun)
    else:
        logging.error("Not enough instances in cluster to reduce size")
        return False
    # Wait for the instances to be drained - only logspout task left
    logging.info("Logging start time...")
    timer_start_utc = datetime.datetime.now(pytz.UTC)
    complete = False
    while not complete:
        for inst in terminate_list[:]:
            if _can_be_terminated(cluster_name, inst):
                result = _terminate_and_remove_from_autoscaling_group(cluster_name, inst, dryrun)
                logging.info(result)
                terminate_list.remove(inst)
        if len(terminate_list) == 0:
            complete = True
        if MAX_WAIT > 0:
            running_time = time_now_utc = datetime.datetime.now(pytz.UTC) - timer_start_utc
            running_time_seconds = running_time.total_seconds()
            running_time_minutes = int(running_time_seconds // 60)
            running_time_hours = int(running_time_seconds // 3600)
            if running_time_hours > MAX_WAIT:
                logging.warning("MAX-WAIT time hit - terminating remaining instances")
                for inst in terminate_list[:]:
                    result = _terminate_and_remove_from_autoscaling_group(cluster_name, inst, dryrun)
                    logging.info(result)
                    terminate_list.remove(inst)
                complete = True
        if not complete:
            logging.debug("Sleeping for 60 seconds")
            time.sleep(60)
    return True


if __name__ == "__main__":

    LOG_FILENAME = 'ecs_cluster_scaledown.log'

    parser = argparse.ArgumentParser(description='ecs_cluster_scaledown')

    parser.add_argument("--aws-access-key-id", help="AWS Access Key ID", dest='aws_access_key', required=False)
    parser.add_argument("--aws-secret-access-key", help="AWS Secret Access Key", dest='aws_secret_key', required=False)
    parser.add_argument("--cluster-name", help="Cluster name", dest='cluster_name', required=True)
    parser.add_argument("--count", help="Number of instances to remove [1]", dest='count', type=int, default=1, required=False)
    parser.add_argument("--instance-id", help="Instance ID to be removed", dest='instance_id', required=False)
    parser.add_argument("--max-wait", help="Maximum wait time (hours) [unlimited]", dest='max_wait', default=0, required=False)
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
    ch = logging.StreamHandler()
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
    ASG = SESSION.client('autoscaling')

    if args.alarm_name:
        cw = SESSION.client('cloudwatch')
        logging.debug('Querying for alarm with name %s in ALARM state in the %s region' % (args.alarm_name, args.region))
        query_result = cw.describe_alarms(AlarmNames=[args.alarm_name], StateValue='ALARM')
        logging.debug(str(query_result))
        matching_alarms = query_result['MetricAlarms']
        logging.debug('Found %s alarms in ALARM state' % str(len(matching_alarms)))
        if len(matching_alarms) == 0:
            logging.error("Given alarm (%s) is NOT in alarm state - will NOT attempt to scale down cluster" % args.alarm_name)
            sys.exit(1)

    MAX_WAIT = 0
    if args.max_wait != 0:
        MAX_WAIT = args.max_wait # Hours

    if MAX_WAIT == 0:
        logging.warning("No MAX-WAIT specified - task could run for a LONG time")
    else:
        logging.warning("MAX-WAIT of %s hour(s) specified - any tasks still running after this time will be killed when the instance is terminated" % args.max_wait)

    if args.instance_id:
        remove_instance_from_ecs_cluster(cluster_name=args.cluster_name,
                                         instance_id=args.instance_id,
                                         dryrun=args.dryrun)
    else:
        scale_down_ecs_cluster(decrease_count=args.count,
                               cluster_name=args.cluster_name,
                               dryrun=args.dryrun)
