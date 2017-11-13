import logging.handlers
import argparse
import boto3
from operator import itemgetter
import datetime
import pytz
import time

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
                    result.extend(_get_instances_in_cluster(cluster_name=cluster_name,
                                                            next_token=query_result['nextToken'],
                                                            status=status))
                else:
                    result.extend(query_result['containerInstanceArns'])
    return result


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
        logging.warning("Dryrun selected - will NOT put instances in DRAINING state")


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
        ASG = SESSION.client('autoscaling')
        if not dryrun:
            activity_result = ASG.terminate_instance_in_auto_scaling_group(InstanceId=instance_id,
                                                                           ShouldDecrementDesiredCapacity=True)
            result += "%s" % activity_result['Activity']['StatusCode']
        else:
            logging.warning("Dryrun selected - no modifications will be done")
            result += "Successful dryrun"
    return result


def scale_down_ecs_cluster(count, cluster_name=None, dryrun=False):
    '''
    Scale down the given ECS cluster by the count given
    :param count: number of instances to remove from cluster
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
    # Get an ordered list of instances in the cluster
    instance_list = _get_instances_in_least_loaded_order(cluster_name=cluster_name)
    logging.debug("Cluster instance list: %s" % str(instance_list))
    instance_count = len(instance_list)
    logging.debug("Cluster instance count: %s" % str(instance_count))
    if instance_count < count:
        # fewer instances present than reduction count
        logging.warn("Given count, %s, greater than current number of instances in the cluster" % str(count))
        logging.warn("Will drain all but 1 instance in the cluster")
        # Drain all but 1 instance
        count = len(instance_list) - 1
    # Drain the least loaded instances
    if instance_count - count > 0:
        terminate_list = instance_list[:count]
        _start_draining_instances(cluster_name, terminate_list, dryrun)
    else:
        logging.error("Not enough instances in cluster to reduce size")
        return False
    # Wait for the instances to be drained - only logspout task left
    if MAX_WAIT > 0:
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
    parser.add_argument("--max-wait", help="Maximum wait time (hours) [unlimited]", dest='max_wait', default=0, required=False)
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

    MAX_WAIT = 0
    if args.max_wait != 0:
        MAX_WAIT = args.max_wait # Hours

    if MAX_WAIT == 0:
        logging.warning("No MAX-WAIT specified - task could run for a LONG time")
    else:
        logging.warning("MAX-WAIT of %s hour(s) specified - any tasks still running after this time will be killed when the instance is terminated" % args.max_wait)

    SESSION = boto3.session.Session(profile_name=args.profile, region_name=args.region)
    ECS = SESSION.client('ecs')

    # TODO: Check the value of a custom cloudwatch metrics to see if scaledown should be attempted or not

    scale_down_ecs_cluster(count=args.count,
                           cluster_name=args.cluster_name,
                           dryrun=args.dryrun)
