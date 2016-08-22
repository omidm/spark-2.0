#!/usr/bin/env python

# Author: Omid Mashayekhi <omidm@stanford.edu>

# ssh -i ~/.ssh/omidm-sing-key-pair-us-west-2.pem -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no ubuntu@<ip>

# EC2 configurations
# US West (Oregon) Region
EC2_LOCATION                    = 'us-west-2'
UBUNTU_AMI                      = 'ami-fa9cf1ca'
SPARK_AMI                       = 'ami-0ce5166c'
KEY_NAME                        = 'omidm-sing-key-pair-us-west-2'
MASTER_INSTANCE_TYPE            = 'c3.4xlarge'
SLAVE_INSTANCE_TYPE             = 'c3.2xlarge'
PLACEMENT                       = 'us-west-2c' # None
PLACEMENT_GROUP                 = 'nimbus-cluster' # None
SECURITY_GROUP                  = 'nimbus_sg_uswest2'
PRIVATE_KEY                     = '/home/omidm/.ssh/' + KEY_NAME + '.pem'
MASTER_NUM                      = 1
SLAVE_NUM                       = 100


# Spark configurations
SLAVE_CORE_NUM                  = 8
EXECUTOR_MEMORY                 = '12g'
APPLICATION                     = 'lr-mllib' # 'lr-mllib' 'lr-rdd' 'kmeans-mllib' 'kmeans-rdd' 
DEACTIVATE_EVENT_LOGING         = False
ACTIVATE_SPARK_INFO_LOGING      = False



# Application configurations
DIMENSION                       = 10
CLUSTER_NUM                     = 2
ITERATION_NUM                   = 30
PARTITION_NUM                   = 8000
SAMPLE_NUM_M                    = 544
SPIN_WAIT_US                    = 0


