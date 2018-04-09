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
* check for any instances in a DRAINING state and attempt to remove them from the cluster (see below)
* get a list of the ACTIVE instances in the given cluster, and order them by load (number of tasks)
* put X number of instances into a DRAINING state
* attempt to remove them from the cluster (see below)

Note: When attempting to remove instances from the cluster, the following conditions need to be met:
* no running tasks on the instance OR
* only tasks matching the ignore list running on the instance

If the above conditions are met, then the following happens:
* terminate the instance(s) and decrement the desired count in the autoscaling group
 
Alternatively, an instance ID can be specified to be selectively removed from the cluster. Note that this
instance MUST be in a DRAINING state already.

# Prerequisites
* Docker must be installed
* Either an AWS role (if running on EC2) or an access key/secret key

# Usage

This tool was developed with the idea of it being run periodically (as an ECS scheduled task, although 
generally speaking, a simple cron job would also work) - that way if an instance wasn't able to be
immediately removed from the cluster (due to running tasks on that instance), the instance would get
removed in a subsequent run of the tool.

The easiest way to run the tool is from docker (because docker rocks).
You will need to pass in variables specific to the ECS task you want to affect

```bash
usage: ecs_cluster_scaledown.py [-h] [--aws-access-key-id AWS_ACCESS_KEY]
                                [--aws-secret-access-key AWS_SECRET_KEY]
                                --cluster-name CLUSTER_NAME [--count COUNT]
                                [--instance-ids INSTANCE_IDS [INSTANCE_IDS ...]]
                                [--ignore-list IGNORE_LIST [IGNORE_LIST ...]]
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
  --instance-ids INSTANCE_IDS [INSTANCE_IDS ...]
                        Instance ID(s) to be removed
  --ignore-list IGNORE_LIST [IGNORE_LIST ...]
                        Tasks to be ignored when determining running tasks
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
       --region us-east-1 \
       --dryrun
```

In this example, the arguments after the image name are

* --cluster-name <ECS cluster name>
* --count <Number of instances to scale down by>
* --region <AWS region>
* --dryrun (don't actually scale down - display only)

In the above example, we tell the task to scale down the cluster by 2 instances. Note that no AWS
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

```bash
docker run \
  signiant/ecs-cluster-scaledown \
        --cluster-name test-cluster \
        --region us-east-1 \
        --ignore-list LogspoutTask
```

In the above example, we tell the task to scale down the cluster by 1 instance (no --count argument was
given, so it uses the default of 1). In this case no alarm is given, so a cluster scale down will be 
always be attempted. (Note that the autoscaling group associated with this cluster is always queried for
the minimum size and a scale down will only be attempted provided it will not result in the cluster size
falling below the minimum) In this case an ignore list is also provided - any tasks matching items in the
given ignore list will be ignored when checking instances for running tasks.

```bash
docker run \
  signiant/ecs-cluster-scaledown \
        --cluster-name test-cluster \
        --region us-east-1 \
        --instance-ids i-0x1x2x3x4x5x6x7x8 i-0x2x3x4x5x6x7x8x9 i-3x4x5x6x7x8x9x0x1
```

In the above example, we tell the task to scale down the cluster by the given instances. Note that 
instances will be removed in the order they are listed.

NOTE: To clear out any instances in a DRAINING state *without* scaling down the cluster, provide a count
of 0, eg:

```bash
docker run \
  signiant/ecs-cluster-scaledown \
        --cluster-name test-cluster \
        --region us-east-1 \
        --ignore-list LogspoutTask
        -- count 0
```


# Warnings / Known Issues

Current only handles clusters that contain instances in a maximum of 2 availability zones.
