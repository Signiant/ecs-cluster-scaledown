# ecs-cluster-scaledown
Scale down a given ECS cluster by a given count

# Purpose
At times it may be desirable to enabled autoscaling of an ECS cluster in only one direction - up. Reasons
for this might include having long running tasks that are indeterministic in duration which don't lend
themselves to being scaled down easily or in the usual manner. Of course the downside to a cluster that
is only enabled to scale up is that the cluster can end up being significantly over provisioned, 
depending on how the autoscaling is configured.

While manually scaling the cluster down is possible, it's certainly a pain. Obviously the solution is
to automate the process. This solution will perform the following steps to scale down an ECS cluster:
* get a list of the instances in the given cluster, and order them by load (number of tasks)
* put X number of instances into a DRAINING state
* wait for the instances to be ready to terminate
* terminate the instances and decrement the desired count in the autoscaling group
 
Alternatively, an instance ID can be specified to be selectively removed from the cluster. Note that this
instance MUST be in a DRAINING state already.

# Prerequisites
* Docker must be installed
* Either an AWS role (if running on EC2) or an access key/secret key

# Usage

The easiest way to run the tool is from docker (because docker rocks).
You will need to pass in variables specific to the ECS task you want to affect

```bash
usage: ecs_cluster_scaledown.py [-h] [--aws-access-key-id AWS_ACCESS_KEY]
                                [--aws-secret-access-key AWS_SECRET_KEY]
                                --cluster-name CLUSTER_NAME [--count COUNT]
                                [--instance-id INSTANCE_ID]
                                [--max-wait MAX_WAIT]
                                [--alarm-name ALARM_NAME] --region REGION
                                [--profile PROFILE] [--verbose] [--dryrun]

ecs_cluster_scaledown

optional arguments:
  -h, --help            show this help message and exit
  --aws-access-key-id AWS_ACCESS_KEY
                        AWS Access Key ID
  --aws-secret-access-key AWS_SECRET_KEY
                        AWS Secret Access Key
  --cluster-name CLUSTER_NAME
                        Cluster name
  --count COUNT         Number of instances to remove [1]
  --instance-id INSTANCE_ID
                        Instance ID to be removed
  --max-wait MAX_WAIT   Maximum wait time (hours) [unlimited]
  --alarm-name ALARM_NAME
                        Alarm name to check if scale down should be attempted
  --region REGION       The AWS region the cluster is in
  --profile PROFILE     The name of an aws cli profile to use.
  --verbose             Turn on DEBUG logging
  --dryrun              Do a dryrun - no changes will be performed
```

# Examples

```bash
docker pull signiant/ecs-cluster-scaledown
```

```bash
docker run \
   signiant/ecs-cluster-scaledown \
       --cluster-name test-cluster \
       --count 2 \
       --max-wait 48
       --region us-east-1 \
       --dryrun
```

In this example, the arguments after the image name are

* --cluster-name <ECS cluster name>
* --count <Number of instances to scale down by>
* --max-wait <max wait time until forcing termination in hours>
* --region <AWS region>
* --dryrun (don't actually scale down - display only)

In the above example, we tell the task to scale down the cluster by 2 instances. If the currently
running tasks on whatever instances are selected (the least loaded at the time this task is started)
are not done executing after 48 hours, the instances will be terminated regardless. Note that no AWS
access key or secret access key, or even an AWS profile are provided - in this case the task was run
as a scheduled task in an ECS cluster using an IAM Role, hence they weren't needed.

To use an AWS access key/secret key rather than a role:

```bash
docker run \
  -e AWS_ACCESS_KEY_ID=XXXXXX \
  -e AWS_SECRET_ACCESS_KEY=XXXXX \
  signiant/ecs-cluster-scaledown \
        --cluster-name test-cluster \
        --region us-east-1 \
        --alarm-name 'Cluster blart scale down'
```

In the above example, we tell the task to scale down the cluster by 1 instance (no --count argument was
given, so it uses the default of 1). The first thing the task will do is check the StateValue of the given
alarm to make sure it is in 'ALARM' state before proceeding - if it isn't, the task exits immediately.
No max-wait argument is given, so this task will wait indefinitely for tasks on the selected instance to
finish before terminating that instance.

# Warnings / Known Issues

While this is selectively terminating instances, if the ECS cluster / Autoscaling group is set up with multiple availability zones, terminating one or more instances in a given availability zone can result in an imbalance between the zones. This, in turn, can result in a new instance being launched to balance the zones, and then a random instance being terminated to keep the cluster size in line with the desired count.
