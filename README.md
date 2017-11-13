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
* get a list of the instances in the given cluster, and ordered them by load (number of tasks)
* put X number of instances into a DRAINING state
* wait for the instances to be ready to terminate
* terminate the instances and decrement the desired count in the autoscaling group
 
# Prerequisites
* Docker must be installed
* Either an AWS role (if running on EC2) or an access key/secret key

# Usage

The easiest way to run the tool is from docker (because docker rocks).
You will need to  pass in variables specific to the ECS task you want to affect

```bash
usage: ecs_cluster_scaledown.py [-h] [--aws-access-key-id AWS_ACCESS_KEY]
                                [--aws-secret-access-key AWS_SECRET_KEY]
                                --cluster-name CLUSTER_NAME [--count COUNT]
                                [--max-wait MAX_WAIT] --region REGION
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
  --max-wait MAX_WAIT   Maximum wait time (hours) [unlimited]
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
       --max-wait 48
       --region us-east-1 \
       --dryrun
```

In this example, the arguments after the image name are

* --cluster-name <ECS cluster name>
* --count <Number of instances to scale down by>
* --max-wait <max wait time until forcing termination in hours>
* --region <AWS region>
* --dryrun (don't actually kill any tasks - display only)

In the above example, we tell the task to scale down the cluster by 1 instance. If the currently 
running tasks on whatever instance is selected (the least loaded at the time this task is started)
are not done executing after 48 hours, the instance will be terminated regardless. Note that no AWS
access key or secret access key, or even an AWS profile are provided - in this case the task was run
on an EC2 instance using an IAM Role, hence they weren't needed.

To use an AWS access key/secret key rather than a role:

```bash
docker run \
  -e AWS_ACCESS_KEY_ID=XXXXXX \
  -e AWS_SECRET_ACCESS_KEY=XXXXX \
  signiant/task-cleanup \
        --task-name-prefix one-time-task \
        --cluster-name test-cluster \
        --region us-east-1 \
```
