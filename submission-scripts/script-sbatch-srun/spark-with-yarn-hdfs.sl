#!/bin/sh
#############################################################################
#  Copyright (C) 2013-2015 Lawrence Livermore National Security, LLC.
#  Produced at Lawrence Livermore National Laboratory (cf, DISCLAIMER).
#  Written by Albert Chu <chu11@llnl.gov>
#  LLNL-CODE-644248
#
#  This file is part of Magpie, scripts for running Hadoop on
#  traditional HPC systems.  For details, see https://github.com/llnl/magpie.
#
#  Magpie is free software; you can redistribute it and/or modify it
#  under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#
#  Magpie is distributed in the hope that it will be useful, but
#  WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#  General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with Magpie.  If not, see <http://www.gnu.org/licenses/>.
#############################################################################

############################################################################
# SLURM Customizations
############################################################################

# Node count.  Node count should include one node for the
# head/management/master node.  For example, if you want 8 compute
# nodes to process data, specify 9 nodes below.
#
# If including Zookeeper, include expected Zookeeper nodes.  For
# example, if you want 8 Hadoop compute nodes and 3 Zookeeper nodes,
# specify 12 nodes (1 master, 8 Hadoop, 3 Zookeeper)
#
# Also take into account additional nodes needed for other services.
#
# Many of the below can be configured on the command line.  If you are
# more comfortable specifying these on the command line, feel free to
# delete the customizations below.

#SBATCH --nodes=8
#SBATCH --output="slurm-%j.out"

# Note defaults of MAGPIE_STARTUP_TIME & MAGPIE_SHUTDOWN_TIME, this
# timelimit should be a fair amount larger than them combined.
#SBATCH --time=3:00:00

# Job name.  This will be used in naming directories for the job.
#SBATCH --job-name=spark-yarn-hdfs-setup

# Partition to launch job in

## SLURM Values
# Generally speaking, don't touch the following, misc other configuration

#SBATCH --ntasks-per-node=1
#SBATCH --exclusive
#SBATCH --no-kill

# Need to tell Magpie how you are submitting this job
export MAGPIE_SUBMISSION_TYPE="sbatchsrun"

# Loading required modules
module load python/3.7.0
module load scipy-stack/2018b

############################################################################
# Magpie Configurations
############################################################################

# Directory your launching scripts/files are stored
#
# Normally an NFS mount, someplace magpie can be reached on all nodes.
export MAGPIE_SCRIPTS_HOME="${HOME}/hadoop/magpie-master"

# Path to store data local to each cluster node, typically something
# in /tmp.  This will store local conf files and log files for your
# job.  If local scratch space is not available, consider using the
# MAGPIE_NO_LOCAL_DIR option.  See README for more details.
#
export MAGPIE_LOCAL_DIR="$SLURM_TMPDIR"

# Magpie job type
#
# "hadoop" - Run a job according to the settings of HADOOP_JOB.
#
# "spark" - Run a job according to the settings of SPARK_JOB.
#
# "testall" - Run a job that runs all basic sanity tests for all
#             software that is configured to be setup.  This is a good
#             way to sanity check that everything has been setup
#             correctly and the way you like.
#
#             For Hadoop, testall will run terasort
#             For Spark, testall will run sparkpi
#
# "script" - Run arbitraty script, as specified by MAGPIE_JOB_SCRIPT.
#            You can find example job scripts in examples/.
#
# "interactive" - manually interact with job run to submit jobs,
#                 peruse data (e.g. HDFS), move data, etc.  See job
#                 output for instructions to access your job
#                 allocation.
#
# "setuponly" - do not launch any daemons or services, only setup
#               configuration files.  Useful for debugging or
#               development.
#
export MAGPIE_JOB_TYPE="interactive"

# Specify script and arguments to execute for "script" mode in
# MAGPIE_JOB_TYPE
#
# export MAGPIE_JOB_SCRIPT="${HOME}/my-job-script"

# Specify script startup / shutdown time window
#
# Specifies the amount of time to give startup / shutdown activities a
# chance to succeed before Magpie will give up (or in the case of
# shutdown, when the resource manager/scheduler may kill the running
# job).  Defaults to 30 minutes for startup, 30 minutes for shutdown.
#
# The startup time in particular may need to be increased if you have
# a large amount of data.  As an example, HDFS may need to spend a
# significant amount of time determine all of the blocks in HDFS
# before leaving safemode.
#
# The stop time in particular may need to be increased if you have a
# large amount of cleanup to be done.  HDFS will save its NameSpace
# before shutting down.  Hbase will do a compaction before shutting
# down.
#
# The startup & shutdown window must together be smaller than the
# timelimit specified for the job.
#
# MAGPIE_STARTUP_TIME and MAGPIE_SHUTDOWN_TIME at minimum must be 5
# minutes.  If MAGPIE_POST_JOB_RUN is specified below,
# MAGPIE_SHUTDOWN_TIME must be at minimum 10 minutes.
#
export MAGPIE_STARTUP_TIME=10
export MAGPIE_SHUTDOWN_TIME=10

# Magpie One Time Run
#
# Normally, Magpie assumes that when a user runs a job, data created
# and stored within that job will be desired to be accessed again.  For
# example, data created and stored within HDFS will be accessed again.
#
# Under a number of scenarios, this may not be desired.  For example
# during testing.
#
# To improve useability and performance, setting MAGPIE_ONE_TIME_RUN
# below to yes will have two effects on the Magpie job.
#
# 1) A number of data paths (such as for HDFS) will be put into unique
#    paths for this job.  Therefore, no other job should be able to
#    access the data again.  This is particularly useful if you wish
#    to run performance tests with this job script over and over
#    again.
#
#    Magpie will not remove data that was written, so be sure to clean up
#    your directories later.
#
# 2) In order to improve job throughout, Magpie will take shortcuts by
#    not properly tearing down the job.  As data corruption should not be
#    a concern on job teardown, the job can complete more quickly.
#
#export MAGPIE_ONE_TIME_RUN=yes

# Convenience Scripts
#
# Specify script to be executed to before / after your job.  It is run
# on all nodes.
#
# Typically the pre-job script is used to set something up or get
# debugging info.  It can also be used to determine if system
# conditions meet the expectations of your job.  The primary job
# running script (magpie-run) will not be executed if the
# MAGPIE_PRE_JOB_RUN exits with a non-zero exit code.
#
# The post-job script is typically used for cleaning up something or
# gathering info (such as logs) for post-debugging/analysis.  If it is
# set, MAGPIE_SHUTDOWN_TIME above must be > 5.
#
# See example magpie-example-pre-job-script and
# magpie-example-post-job-script for ideas of what you can do w/ these
# scripts
#
# Multiple scripts can be specified separated by comma.  Arguments can
# be passed to scripts as well.
#
# A number of convenient scripts are available in the
# ${MAGPIE_SCRIPTS_HOME}/scripts directory.
#
export MAGPIE_PRE_JOB_RUN="${MAGPIE_SCRIPTS_HOME}/scripts/pre-job-run-scripts/python_start.sh"
# export MAGPIE_POST_JOB_RUN="${MAGPIE_SCRIPTS_HOME}/scripts/post-job-run-scripts/my-post-job-script"

# Environment Variable Script
#
# When working with Magpie interactively by logging into the master
# node of your job allocation, many environment variables may need to
# be set.  For example, environment variables for config file
# directories (e.g. HADOOP_CONF_DIR, HBASE_CONF_DIR, etc.) and home
# directories (e.g. HADOOP_HOME, HBASE_HOME, etc.) and more general
# environment variables (e.g. JAVA_HOME) may need to be set before you
# begin interacting with your big data setup.
#
# The standard job output from Magpie provides instructions on all the
# environment variables typically needed to interact with your job.
# However, this can be tedious if done by hand.
#
# If the environment variable specified below is set, Magpie will
# create the file and put into it every environment variable that
# would be useful when running your job interactively.  That way, it
# can be sourced easily if you will be running your job interactively.
# It can also be loaded or used by other job scripts.
#
# export MAGPIE_ENVIRONMENT_VARIABLE_SCRIPT="${HOME}/my-job-env"

# Environment Variable Shell Type
#
# Magpie outputs environment variables in help output and
# MAGPIE_ENVIRONMENT_VARIABLE_SCRIPT based on your SHELL environment
# variable.
#
# If you would like to output in a different shell type (perhaps you
# have programmed scripts in a different shell), specify that shell
# here.
#
# export MAGPIE_ENVIRONMENT_VARIABLE_SCRIPT_SHELL="/bin/bash"

# Remote Shell
#
# Magpie requires a passwordless remote shell command to launch
# necessary daemons across your job allocation.  Magpie defaults to
# ssh, but it may be an alternate command in some environments.  An
# alternate ssh-equivalent remote command can be specified by setting
# MAGPIE_REMOTE_CMD below.
#
# If using ssh, Magpie requires keys to be setup ahead of time so it
# can be executed without passwords.
#
# Specify options to the remote shell command if necessary.
#
# export MAGPIE_REMOTE_CMD="ssh"
# export MAGPIE_REMOTE_CMD_OPTS=""

############################################################################
# General Configuration
############################################################################

# Necessary for most projects
export JAVA_HOME="/usr/lib/jvm/jre-1.8.0/"

# MAGPIE_PYTHON path used for:
# - Spark PySpark path
# - Launching tensorflow tasks
export MAGPIE_PYTHON="/cvmfs/soft.computecanada.ca/easybuild/software/2017/Core/python/3.7.0/bin/python"

############################################################################
# Hadoop Core Configurations
############################################################################

# Should Hadoop be run
#
# Specify yes or no.  Defaults to no.
#
export HADOOP_SETUP=yes

# Set Hadoop Setup Type
#
# Will inform scripts on how to setup config files and what daemons to
# launch/setup.
#
# MR - Launch HDFS and Yarn
# YARN - Enable only Yarn
# HDFS - Enable only HDFS
#
# HDFS only may be useful when you want to use HDFS with other big
# data software, such as Hbase, and do not care for MapReduce or Yarn.
# It only works with HDFS based HADOOP_FILESYSTEM_MODE, such as
# "hdfs", "hdfsoverlustre", or "hdfsovernetworkfs".
#
# YARN only may be useful when you need Yarn setup for scheduling, but
# will not be using HDFS.  For example, you may be reading from a
# networked file system directly.  This option requires
# HADOOP_FILESYSTEM_MODE to 'rawnetworkfs'.
#
export HADOOP_SETUP_TYPE="MR"

# Version
#
# Make sure the version for Mapreduce version 1 or 2 matches whatever
# you set in HADOOP_SETUP_TYPE
#
export HADOOP_VERSION="2.7.7"

# Path to your Hadoop build/binaries
#
# Make sure the build for MapReduce or HDFS version 1 or 2 matches
# whatever you set in HADOOP_SETUP_TYPE.
#
# This should be accessible on all nodes in your allocation. Typically
# this is in an NFS mount.
#
export HADOOP_HOME="${HOME}/hadoop/hadoop-${HADOOP_VERSION}"

# Path to store data local to each cluster node, typically something
# in /tmp.  This will store local conf files and log files for your
# job.  If local scratch space is not available, consider using the
# MAGPIE_NO_LOCAL_DIR option.  See README for more details.
#
# This will not be used for storing intermediate files or
# distributed cache files.  See HADOOP_LOCALSTORE above for that.
#
export HADOOP_LOCAL_DIR="$SLURM_TMPDIR/hadoop"

# Directory where alternate Hadoop configuration templates are stored
#
# If you wish to tweak the configuration files used by Magpie, set
# HADOOP_CONF_FILES below, copy configuration templates from
# $MAGPIE_SCRIPTS_HOME/conf/hadoop into HADOOP_CONF_FILES, and modify
# as you desire.  Magpie will still use configuration files in
# $MAGPIE_SCRIPTS_HOME/conf/hadoop if any of the files it needs are
# not found in HADOOP_CONF_FILES.
#
# export HADOOP_CONF_FILES="${HOME}/myconf"

# Daemon Heap Max
#
# Heap maximum for Hadoop daemons (i.e. Resource Manger, NodeManager,
# DataNode, History Server, etc.), specified in megs.  Special case
# for Namenode, see below.
#
# If not specified, defaults to Hadoop default of 1000
#
# May need to be increased if you are scaling large, get OutofMemory
# errors, or perhaps have a lot of cores on a node.
#
#export HADOOP_DAEMON_HEAP_MAX=2000

# Daemon Namenode Heap Max
#
# Heap maximum for Hadoop Namenode daemons specified in megs.
#
# If not specified, defaults to HADOOP_DAEMON_HEAP_MAX above.
#
# Unlike most Hadoop daemons, namenode may need more memory if there
# are a very large number of files in your HDFS setup.  A general rule
# of thumb is a 1G heap for each 100T of data.
#
# export HADOOP_NAMENODE_DAEMON_HEAP_MAX=2000

# Environment Extra
#
# Specify extra environment information that should be passed into
# Hadoop.  This file will simply be appended into the hadoop-env.sh
# and (if appropriate) yarn-env.sh.
#
# By default, a reasonable estimate for max user processes and open
# file descriptors will be calculated and put into hadoop-env.sh and
# (if appropriate) yarn-env.sh.  However, it's always possible they may
# need to be set differently. Everyone's cluster/situation can be
# slightly different.
#
# See the example example-environment-extra extra for examples on
# what you can/should do with adding extra environment settings.
#
export HADOOP_ENVIRONMENT_EXTRA_PATH="${HOME}/hadoop/python_start"

############################################################################
# Hadoop Job/Run Configurations
############################################################################

# Set hadoop job for MAGPIE_JOB_TYPE = hadoop
#
# "terasort" - run terasort.  Useful for making sure things are setup
#              the way you like.
#
#              There are additional configuration options for this
#              listed below.
#
# "upgradehdfs" - upgrade your version of HDFS.  Most notably this is
#                 used when you are switching to a newer Hadoop
#                 version and the HDFS version would be inconsistent
#                 without upgrading.  Only works with HDFS versions >=
#                 2.2.0.
#
#	          Please set your job time to be quite large when
#		  performing this upgrade.  If your job times out and
#		  this process does not complete fully, it can leave
#		  HDFS in a bad state.
#
#		  Beware, once you upgrade it'll be difficult to rollback.
#
# "decommissionhdfsnodes" - decrease your HDFS over Lustre or HDFS
#                           over NetworkFS node size just as if you
#                           were on a cluster with local disk.  Launch
#                           your job with the current present node
#                           size and set
#                           HADOOP_DECOMMISSION_HDFS_NODE_SIZE to the
#                           smaller node size to decommission into.
#                           Only works on Hadoop versions >= 2.3.0.
#
#		            Please set your job time to be quite large
#		            when performing this update.  If your job
#		            times out and this process does not
#		            complete fully, it can leave HDFS in a bad
#		            state.
#
export HADOOP_JOB="terasort"

# Tasks per Node
#
# If not specified, a reasonable estimate will be calculated based on
# number of CPUs on the system.
#
# If running Hbase (or other Big Data software) with Hadoop MapReduce,
# be aware of the number of tasks and the amount of memory that may be
# needed by other software.
#
# export HADOOP_MAX_TASKS_PER_NODE=8

# Default Map tasks for Job
#
# If not specified, defaults to HADOOP_MAX_TASKS_PER_NODE * compute
# nodes.
#
# If running Hbase (or other Big Data software) with Hadoop MapReduce,
# be aware of the number of tasks and the amount of memory that may be
# needed by other software.
#
# export HADOOP_DEFAULT_MAP_TASKS=8

# Default Reduce tasks for Job
#
# If not specified, defaults to # compute nodes (i.e. 1 reducer per
# node)
#
# If running Hbase (or other Big Data software) with Hadoop MapReduce,
# be aware of the number of tasks and the amount of memory that may be
# needed by other software.
#
# export HADOOP_DEFAULT_REDUCE_TASKS=8

# Heap size for JVM
#
# Specified in M.  If not specified, a reasonable estimate will be
# calculated based on total memory available and number of CPUs on the
# system.
#
# HADOOP_CHILD_MAP_HEAPSIZE and HADOOP_CHILD_REDUCE_HEAPSIZE are for
# Yarn
#
# If HADOOP_CHILD_MAP_HEAPSIZE is not specified, it is assumed to be
# HADOOP_CHILD_HEAPSIZE.
#
# If HADOOP_CHILD_REDUCE_HEAPSIZE is not specified, it is assumed to
# be 2X the HADOOP_CHILD_MAP_HEAPSIZE.
#
# If running Hbase (or other Big Data software) with Hadoop MapReduce,
# be aware of the number of tasks and the amount of memory that may be
# needed by other software.
#
# export HADOOP_CHILD_HEAPSIZE=2048
# export HADOOP_CHILD_MAP_HEAPSIZE=2048
# export HADOOP_CHILD_REDUCE_HEAPSIZE=4096

# Container Buffer
#
# Specify the amount of overhead each Yarn container will have over
# the heap size.  Specified in M.  If not specified, a reasonable
# estimate will be calculated based on total memory available.
#
# export HADOOP_CHILD_MAP_CONTAINER_BUFFER=256
# export HADOOP_CHILD_REDUCE_CONTAINER_BUFFER=512

# Mapreduce Slowstart, indicating percent of maps that should complete
# before reducers begin.
#
# If not specified, defaults to 0.05
#
# export HADOOP_MAPREDUCE_SLOWSTART=0.05

# Container Memory
#
# Memory on compute nodes for containers.  Typically "nice-chunk" less
# than actual memory on machine, b/c machine needs memory for its own
# needs (kernel, daemons, etc.).  Specified in megs.
#
# If not specified, a reasonable estimate will be calculated based on
# total memory on the system.
#
# export YARN_RESOURCE_MEMORY=32768

# Check Memory Limits
#
# Should physical and virtual memory limits be enforced for containers.
# This can be helpful in cases where the OS (Centos/Redhat) is aggressive
# at allocating virtual memory and causes the vmem-to-pmem ratio to be
# hit. Defaults to true
#
# export YARN_VMEM_CHECK="false"
# export YARN_PMEM_CHECK="false"

# Compression
#
# Should compression of outputs and intermediate data be enabled.
# Specify yes or no.  Defaults to no.
#
# Effectively, is time spend compressing data going to save you time
# on I/O.  Sometimes yes, sometimes no.
#
# export HADOOP_COMPRESSION=yes

# IO Sort Factors + MB
#
# The number of streams of files to sort while reducing and the memory
# amount to use while sorting.  This is a quite advanced mechanism
# taking into account many factors.  If not specified, some reasonable
# number will be calculated.
#
# export HADOOP_IO_SORT_FACTOR=10
# export HADOOP_IO_SORT_MB=100

# Parallel Copies
#
# The default number of parallel transfers run by reduce during the
# copy(shuffle) phase.  If not specified, some reasonable number will
# be calculated.
# export HADOOP_PARALLEL_COPIES=10

############################################################################
# Hadoop Terasort Configurations
############################################################################

# Terasort size
#
# For "terasort" mode.
#
# Specify terasort size in units of 100.  Specify 10000000000 for
# terabyte, for actual benchmarking
#
# Specify something small, for basic sanity tests.
#
# Defaults to 50000000.
#
# export HADOOP_TERASORT_SIZE=50000000

# Terasort map count
#
# For "terasort" mode during the teragen of data.
#
# If not specified, will be computed to a reasonable number given
# HADOOP_TERASORT_SIZE and the block size of the the filesyste you are
# using (e.g. for HDFS the HADOOP_HDFS_BLOCKSIZE)
#
# export HADOOP_TERAGEN_MAP_COUNT=4

# Terasort reducer count
#
# For "terasort" mode during the actual terasort of data.
#
# If not specified, will be compute node count * 2.
#
# export HADOOP_TERASORT_REDUCER_COUNT=4

# Terasort cache
#
# For "real benchmarking" you should flush page cache between a
# teragen and a terasort.  You can disable this for sanity runs/tests
# to make things go faster.  Specify yes or no.  Defaults to yes.
#
# export HADOOP_TERASORT_CLEAR_CACHE=no

# Terasort output replication count
#
# For "terasort" mode during the actual terasort of data
#
# In some circumstances, replication of the output from the terasort
# must be equal to the replication of data for the input.  In other
# cases it can be less.  The below can be adjusted to tweak for
# benchmarking purposes.
#
# If not specified, defaults to Terasort default, which is 1 in most
# versions of Hadoop
#
# export HADOOP_TERASORT_OUTPUT_REPLICATION=1

# Terachecksum
#
# For "terasort" mode after the teragen of data
#
# After executing the teragen, run terachecksum to calculate a checksum of
# the input.
#
# If both this and HADOOP_TERASORT_RUN_TERAVALIDATE are set, the
# checksums will be compared afterwards for equality.
#
# Defaults to no
#
# export HADOOP_TERASORT_RUN_TERACHECKSUM=no

# Teravalidate
#
# For "terasort" mode after the actual terasort of data
#
# After executing the sort, run teravalidate to validate the sorted data.
#
# If both this and HADOOP_TERASORT_RUN_TERACHECKSUM are set, the
# checksums will be compared afterwards for equality.
#
# Defaults to no
#
# export HADOOP_TERASORT_RUN_TERAVALIDATE=no

############################################################################
# Hadoop Decommission HDFS Nodes Configurations
############################################################################

# Specify decommission node size for "decommissionhdfsnodes" mode
#
# For example, if your current HDFS node size is 16, your job size is
# likely 17 nodes (including the master).  If you wish to decommission
# to 8 data nodes (job size of 9 nodes total), set this to 8.
#
# export HADOOP_DECOMMISSION_HDFS_NODE_SIZE=8

############################################################################
# Hadoop Filesystem Mode Configurations
############################################################################

# Set how the filesystem should be setup
#
# "hdfs" - Normal straight up HDFS if you have local disk in your
#          cluster.  This option is primarily for benchmarking and
#          caching, but probably shouldn't be used in the general case.
#
#          Be careful running this in a cluster environment.  The next
#          time you execute your job, if a different set of nodes are
#          allocated to you, the HDFS data you wrote from a previous
#          job may not be there.  Specifying specific nodes to use in
#          your job submission (e.g. --nodelist in sbatch) may be a
#          way to alleviate this.
#
#          User must set HADOOP_HDFS_PATH below.
#
# "hdfsoverlustre" - HDFS over Lustre.  See README for description.
#
#                    User must set HADOOP_HDFSOVERLUSTRE_PATH below.
#
# "hdfsovernetworkfs" - HDFS over Network FS.  Identical to HDFS over
#                       Lustre, but filesystem agnostic.
#
#                       User must set HADOOP_HDFSOVERNETWORKFS_PATH below.
#
# "rawnetworkfs" - Use Hadoop RawLocalFileSystem (i.e. file: scheme),
#           to use networked file system directly.  It could be a
#           Lustre mount or NFS mount.  Whatever you please.
#
#           User must set HADOOP_RAWNETWORKFS_PATH below.
#
export HADOOP_FILESYSTEM_MODE="hdfsoverlustre"

# Local Filesystem BlockSize
#
# This configuration is the blocksize hadoop will use when doing I/O
# to a local filesystem.  It is used by HDFS when reading from the
# underlying filesystem.  It is also used with
# HADOOP_FILESYSTEM_MODE="rawnetworkfs".
#
# Commonly 33554432, 67108864, 134217728 (i.e. 32m, 64m, 128m)
#
# If not specified, defaults to 33554432
#
# export HADOOP_LOCAL_FILESYSTEM_BLOCKSIZE=33554432

# HDFS Replication
#
# This is used with HADOOP_FILESYSTEM_MODE="hdfs", "hdfsoverlustre",
# and "hdfsovernetworkfs"
#
# HDFS commonly uses 3.  When doing HDFS over Lustre/NetworkFS, higher
# replication can also help with resilience if nodes fail.  You may
# wish to set this to < 3 to save space.
#
# If not specified, defaults to 3
#
# export HADOOP_HDFS_REPLICATION=3

# HDFS Block Size
#
# This is used with HADOOP_FILESYSTEM_MODE="hdfs", "hdfsoverlustre",
# and "hdfsovernetworkfs"
#
# Commonly 134217728, 268435456, 536870912 (i.e. 128m, 256m, 512m)
#
# If not specified, defaults to 134217728
#
# export HADOOP_HDFS_BLOCKSIZE=134217728

# Path for HDFS when using local disk
#
# This is used with HADOOP_FILESYSTEM_MODE="hdfs"
#
# If you want to specify multiple paths (such as multiple drives),
# make them comma separated (e.g. /dir1,/dir2,/dir3).  The multiple
# paths will be used for local intermediate data and HDFS.  The first
# path will also store daemon data, such as namenode or jobtracker
# data.
#
export HADOOP_HDFS_PATH="/ssd/${USER}/hdfs"

# HDFS cleanup
#
# This is used with HADOOP_FILESYSTEM_MODE="hdfs"
#
# After your job has completed, if HADOOP_HDFS_PATH_CLEAR is set to
# yes, Magpie will do a rm -rf on HADOOP_HDFS_PATH.
#
# This is particularly useful when doing normal HDFS on local storage.
# On your next job run, you may not be able to get the nodes you want
# on your next run.  So you may want to clean up your work before the
# next user uses the node.
#
# export HADOOP_HDFS_PATH_CLEAR="yes"

# Lustre path to do Hadoop HDFS out of
#
# This is used with HADOOP_FILESYSTEM_MODE="hdfsoverlustre"
#
# Note that different versions of Hadoop may not be compatible with
# your current HDFS data.  If you're going to switch around to
# different versions, perhaps set different paths for different data.
# According to the magpie documentation "instead of using local disk,
# we designate a Lustre/network file system directory to emulate
# local disk for each compute node." Based on CC documentation
# "As far as possible, use scratch and local storage for temporary
# files."
export HADOOP_HDFSOVERLUSTRE_PATH="/home/${USER}/scratch/laagi/hdfsoverlustre/"

# HDFS over Lustre ignore lock
#
# This is used with HADOOP_FILESYSTEM_MODE="hdfsoverlustre"
#
# Cleanup in_use.lock files before launching HDFS
#
# On traditional Hadoop clusters, the in_use.lock file protects
# against a second HDFS daemon running on the same node.  The lock
# file can similarly protect against a second HDFS daemon running on
# another node of your cluster (which is not desired, as both
# namenodes could change namenode data at the same time).
#
# However, sometimes the lock file may be there due to a prior job
# that failed and locks were not cleaned up on teardown.  This may
# prohibit new HDFS daemons from running correctly.
#
# By default, if this option is not set, the lock file will be left in
# place and may cause HDFS daemons to not start.  If set to yes, the
# lock files will be removed before starting HDFS.
#
# export HADOOP_HDFSOVERLUSTRE_REMOVE_LOCKS=yes

# Networkfs path to do Hadoop HDFS out of
#
# This is used with HADOOP_FILESYSTEM_MODE="hdfsovernetworkfs"
#
# Note that different versions of Hadoop may not be compatible with
# your current HDFS data.  If you're going to switch around to
# different versions, perhaps set different paths for different data.
#
export HADOOP_HDFSOVERNETWORKFS_PATH="/networkfs/${USER}/hdfsovernetworkfs/"

# HDFS over Networkfs ignore lock
#
# This is used with HADOOP_FILESYSTEM_MODE="hdfsovernetworkfs"
#
# Cleanup in_use.lock files before launching HDFS
#
# On traditional Hadoop clusters, the in_use.lock file protects
# against a second HDFS daemon running on the same node.  The lock
# file can similarly protect against a second HDFS daemon running on
# another node of your cluster (which is not desired, as both
# namenodes could change namenode data at the same time).
#
# However, sometimes the lock file may be there due to a prior job
# that failed and locks were not cleaned up on teardown.  This may
# prohibit new HDFS daemons from running correctly.
#
# By default, if this option is not set, the lock file will be left in
# place and may cause HDFS daemons to not start.  If set to yes, the
# lock files will be removed before starting HDFS.
#
# export HADOOP_HDFSOVERNETWORKFS_REMOVE_LOCKS=yes

# Path for rawnetworkfs
#
# This is used with HADOOP_FILESYSTEM_MODE="rawnetworkfs"
#
export HADOOP_RAWNETWORKFS_PATH="/lustre/${USER}/rawnetworkfs/"

# If you have a local SSD or NVRAM, performance may be better to store
# intermediate data on it rather than Lustre or some other networked
# filesystem.  If the below environment variable is specified, local
# intermediate data will be stored in the specified directory.
# Otherwise it will go to an appropriate directory in Lustre/networked
# FS.
#
# Be wary, local SSDs/NVRAM stores may have less space than HDDs or
# networked file systems.  It can be easy to run out of space.
#
# If you want to specify multiple paths (such as multiple drives),
# make them comma separated (e.g. /dir1,/dir2,/dir3).  The multiple
# paths will be used for local intermediate data.
#
# export HADOOP_LOCALSTORE="/ssd/${USER}/localstore/"

# HADOOP_LOCALSTORE_CLEAR
#
# After your job has completed, if HADOOP_LOCALSTORE_CLEAR is set to
# yes, Magpie will do a rm -rf on all directories in
# HADOOP_LOCALSTORE.  This is particularly useful if the localstore
# directory is on local storage and you want to clean up your work
# before the next user uses the node.
#
# export HADOOP_LOCALSTORE_CLEAR="yes"
############################################################################
# Spark Core Configurations
############################################################################

# Should Spark be run
#
# Specify yes or no.  Defaults to no.
#
export SPARK_SETUP=yes

# Set Spark Setup Type
#
# Will inform scripts on how to setup config files and what daemons to
# launch/setup.
#
# STANDALONE - Launch Spark stand alone scheduler
# YARN - do not setup stand alone scheduler, use Yarn scheduler
#
# For most users, the stand alone scheulder is likely preferred.
# Resources do not need to be competed for in a Magpie environment
# (you the user have all the resources allocated via the
# scheduler/resource manager of your cluster).
#
# However, portability and integration with other services may require
# Spark to work with Yarn instead.  If SPARK_SETUP_TYPE is set to
# YARN:
#
# - The Spark standalone scheduler will not be launched
# - The default Spark master will be set to 'yarn-client' in all
#   appropriate locations (e.g. spark-defaults.conf)
# - All situations that would otherwise use the standalone scheduler
#   (e.g. SPARK_JOB="sparkpi") will now use Yarn instead.
#
# Make sure that HADOOP_SETUP_TYPE is set to MR or YARN for the
# SPARK_SETUP_TYPE="YARN".
#
export SPARK_SETUP_TYPE="YARN"

# Version
#
export SPARK_VERSION="2.4.0-bin-hadoop2.7"

# Path to your Spark build/binaries
#
# This should be accessible on all nodes in your allocation. Typically
# this is in an NFS mount.
#
# Ensure the build matches the Hadoop/HDFS version this will run against.
#
export SPARK_HOME="${HOME}/hadoop/spark-${SPARK_VERSION}"

# Path to store data local to each cluster node, typically something
# in /tmp.  This will store local conf files and log files for your
# job.  If local scratch space is not available, consider using the
# MAGPIE_NO_LOCAL_DIR_DIR option.  See README for more details.
#
export SPARK_LOCAL_DIR="$SLURM_TMPDIR/spark"

# Directory where alternate Spark configuration templates are stored
#
# If you wish to tweak the configuration files used by Magpie, set
# SPARK_CONF_FILES below, copy configuration templates from
# $MAGPIE_SCRIPTS_HOME/conf/spark into SPARK_CONF_FILES, and modify as
# you desire.  Magpie will still use configuration files in
# $MAGPIE_SCRIPTS_HOME/conf/spark if any of the files it needs are not
# found in SPARK_CONF_FILES.
#
# export SPARK_CONF_FILES="${HOME}/myconf"

# Worker Cores per Node
#
# If not specified, a reasonable estimate will be calculated based on
# number of CPUs on the system.
#
# Be aware of the number of tasks and the amount of memory that may be
# needed by other software.
#
# export SPARK_WORKER_CORES_PER_NODE=8

# Worker Memory
#
# Specified in M.  If not specified, a reasonable estimate will be
# calculated based on total memory available and number of CPUs on the
# system.
#
# Be aware of the number of tasks and the amount of memory that may be
# needed by other software.
#
# export SPARK_WORKER_MEMORY_PER_NODE=16000

# Worker Directory
#
# Directory to run applications in, which will include both logs and
# scratch space for local jars.  If not specified, defaults to
# SPARK_LOCAL_DIR/work.
#
# Generally speaking, this is best if this is a tmp directory such as
# in /tmp
#
# export SPARK_WORKER_DIRECTORY=/tmp/${USER}/spark/work

# SPARK_JOB_MEMORY
#
# Memory for spark jobs.  Defaults to being set equal to
# SPARK_WORKER_MEMORY_PER_NODE, but users may wish to lower it if
# multiple Spark jobs will be submitted at the same time.
#
# In Spark parlance, this will set both the executor and driver memory
# for Spark.
#
# export SPARK_JOB_MEMORY="2048"

# SPARK_DRIVER_MEMORY
#
# Beginning in Spark 1.0, driver memory could be configured separately
# from executor memory.  If SPARK_DRIVER_MEMORY is set below, driver
# memory will be configured differently than the executor memory
# indicated above with SPARK_JOB_MEMORY.
#
# If running Spark < 1.0, this option does nothing.
#
# export SPARK_DRIVER_MEMORY="2048"

# Daemon Heap Max
#
# Heap maximum for Spark daemons, specified in megs.
#
# If not specified, defaults to 1000
#
# May need to be increased if you are scaling large, get OutofMemory
# errors, or perhaps have a lot of cores on a node.
#
export SPARK_DAEMON_HEAP_MAX=2000

# Environment Extra
#
# Specify extra environment information that should be passed into
# Spark.  This file will simply be appended into the spark-env.sh.
#
# By default, a reasonable estimate for max user processes and open
# file descriptors will be calculated and put into spark-env.sh.
# However, it's always possible they may need to be set
# differently. Everyone's cluster/situation can be slightly different.
#
# See the example example-environment-extra for examples on
# what you can/should do with adding extra environment settings.
#
export SPARK_ENVIRONMENT_EXTRA_PATH="${HOME}/hadoop/python_start"

############################################################################
# Spark Job/Run Configurations
############################################################################

# Set spark job for MAGPIE_JOB_TYPE = spark
#
# "sparkpi" - run sparkpi example. Useful for making sure things are
#             setup the way you like.
#
#             There are additional configuration options for this
#             example listed below.
#
# "sparkwordcount" - run wordcount example.  Useful for making sure
#                    things are setup the way you like.
#
#                    See below for additional required configuration
#                    for this example.
#
export SPARK_JOB="sparkpi"

# SPARK_DEFAULT_PARALLELISM
#
# Default number of tasks to use across the cluster for distributed
# shuffle operations (groupByKey, reduceByKey, etc) when not set by
# user.
#
# For Spark versions < 1.3.0.  Defaults to number compute nodes
# (i.e. 1 per node) This is something you (the user) almost definitely
# want to set as this is non-optimal for most jobs.
#
# For Spark versions >= 1.3.0, will not be set by Magpie if the below
# is commented out.  Internally, Spark defaults this to the largest
# number of partitions in a parent RDD.  Or for operations without a
# parent, defaults to nodes X cores.
#
# export SPARK_DEFAULT_PARALLELISM=8

# SPARK_MEMORY_FRACTION
#
# Fraction of heap space used for execution and storage. The lower
# this is, the more frequently spills and cached data eviction occur.
#
# This configuration only works for Spark versions >= 1.6.0.  See
# SPARK_STORAGE_MEMORY_FRACTION and SPARK_SHUFFLE_MEMORY_FRACTION for
# older versions.
#
# Defaults to 0.6
#
# export SPARK_MEMORY_FRACTION=0.6

# SPARK_MEMORY_STORAGE_FRACTION
#
# Amount of storage memory immune to eviction, expressed as a fraction
# of the size of the region set aside by SPARK_MEMORY_FRACTION.  The
# higher this is, the less working memory may be available to
# executiona nd tasks may spill to disk more often.
#
# This configuration only works for Spark versions >= 1.6.0.  See
# SPARK_STORAGE_MEMORY_FRACTION and SPARK_SHUFFLE_MEMORY_FRACTION for
# older versions.
#
# Defaults to 0.5
#
# export SPARK_MEMORY_STORAGE_FRACTION=0.5

# SPARK_STORAGE_MEMORY_FRACTION
#
# Configure fraction of Java heap to use for Spark's memory cache.
# This should not be larger than the "old" generation of objects in
# the JVM.  This can highly affect performance due to interruption due
# to JVM garbage collection.  If a large amount of time is spent in
# garbage collection, consider shrinking this value, such as to 0.5 or
# 0.4.
#
# This configuration only works for Spark versions < 1.6.0.  Starting
# with 1.6.0, see SPARK_MEMORY_FRACTION and
# SPARK_MEMORY_STORAGE_FRACTION.
#
# Defaults to 0.6
#
# export SPARK_STORAGE_MEMORY_FRACTION=0.6

# SPARK_SHUFFLE_MEMORY_FRACTION
#
# Fraction of Java heap to use for aggregation and cogroups during
# shuffles.  At any given time, the collective size of all in-memory
# maps used for shuffles is bounded by this limit, beyond which the
# contents will begin to spill to disk.  If spills are often, consider
# increasing this value at the expense of storage memory fraction
# (SPARK_STORAGE_MEMORY_FRACTION above).
#
# This configuration only works for Spark versions < 1.6.0.  Starting
# with 1.6.0, see SPARK_MEMORY_FRACTION and
# SPARK_MEMORY_STORAGE_FRACTION.
#
# Defaults to 0.2
#
# export SPARK_SHUFFLE_MEMORY_FRACTION=0.2

# SPARK_RDD_COMPRESS
#
# Should RDD's be compressed by default?  Defaults to true.  In HPC
# environments with parallel file systems or local storage, the cost
# of compressing / decompressing RDDs is likely a be a net win over
# writing data to a parallel file system or using up the limited
# amount of local storage space available.  However, for some users
# this cost may not be worthwhile and should be disabled.
#
# Note that only serialized RDDs are compressed, such as with storage
# level MEMORY_ONLY_SER or MEMORY_AND_DISK_SER.  All python RDDs are
# serialized by default.
#
# Defaults to true.
#
# export SPARK_RDD_COMPRESS=true

# SPARK_DEPLOY_SPREADOUT
#
# Per Spark documentation, "Whether the standalone cluster manager
# should spread applications out across nodes or try to consolidate
# them onto as few nodes as possible. Spreading out is usually better
# for data locality in HDFS, but consolidating is more efficient for
# compute-intensive workloads."
#
# If you are hard coding parallelism in certain parts of your
# application because those individual actions do not scale well, it
# may be beneficial to disable this.
#
# Defaults to true
#
# export SPARK_DEPLOY_SPREADOUT=true

# SPARK_JOB_CLASSPATH
#
# May be necessary to set to get certain code/scripts to work.
#
# e.g. to run a Spark example, you may need to set
#
# export SPARK_JOB_CLASSPATH="examples/target/spark-examples-assembly-0.9.1.jar"
#
# Note that this is primarily for Spark 0.9.1 and earlier versions.
# You likely want to use the --driver-class-path option in
# spark-submit now.
#
# export SPARK_JOB_CLASSPATH=""

# SPARK_JOB_LIBRARY_PATH
#
# May be necessary to set to get certain code/scripts to work.
#
# Note that this is primarily for Spark 0.9.1 and earlier versions.
# You likely want to use the --driver-library-path option in
# spark-submit now.
#
# export SPARK_JOB_LIBRARY_PATH=""

# SPARK_JOB_JAVA_OPTS
#
# May be necessary to set options to set Spark options
#
# e.g. -Dspark.default.parallelism=16
#
# Magpie will set several options on its own, however, these options
# will be appended last, ensuring they override anything that Magpie
# will set by default.
#
# Note that many of the options that were set in SPARK_JAVA_OPTS in
# Spark 0.9.1 and earlier have been deprecated.  You likely want to
# use the --driver-java-options option in spark-submit now.
#
# export SPARK_JOB_JAVA_OPTS=""

# SPARK_LOCAL_SCRATCH_DIR
#
# By default, if Hadoop is setup with a file system, the Spark local
# scratch directory, where scratch data is placed, will automatically
# be calculated and configured.  If Hadoop is not setup, the following
# must be specified.
#
# If you have local SSDs or NVRAM stored on the nodes of your system,
# it may be in your interest to set this to a local drive.  It can
# improve performance of both shuffling and disk based RDD
# persistence.  If you want to specify multiple paths (such as
# multiple drives), make them comma separated
# (e.g. /dir1,/dir2,/dir3).
#
# Note that this field will not work if SPARK_SETUP_TYPE="YARN".
# Please set HADOOP_LOCALSTORE to inform Yarn to set a local SSD for
# Yarn to use for local scratch.
#
# export SPARK_LOCAL_SCRATCH_DIR="/lustre/${USER}/sparkscratch/"

# SPARK_LOCAL_SCRATCH_DIR_CLEAR
#
# After your job has completed, if SPARK_LOCAL_SCRATCH_DIR_CLEAR is
# set to yes, Magpie will do a rm -rf on all directories in
# SPARK_LOCAL_SCRATCH_DIR.  This is particularly useful if the local
# scratch directory is on local storage and you want to clean up your
# work before the next user uses the node.
#
# export SPARK_LOCAL_SCRATCH_DIR_CLEAR="yes"

# SPARK_NETWORK_TIMEOUT
#
# This will configure many network timeout configurations within
# Spark.  If you see that your jobs are regularly failing with timeout
# issues, try increasing this value.  On large HPC systems, timeouts
# may occur more often, such as on loaded parallel file systems or
# busy networks.
#
# As of this version, this will configure:
#
# spark.network.timeout
# spark.files.fetchTimeout
# spark.rpc.askTimeout
# spark.rpc.lookupTimeout
# spark.core.connection.ack.wait.timeout
# spark.shuffle.registration.timeout
# spark.network.auth.rpcTimeout
# spark.shuffle.sasl.timeout
#
# Note that some of this fields will only effect newer spark versions.
#
# Specified in seconds, by default 120.
#
# export SPARK_NETWORK_TIMEOUT=120

# SPARK_YARN_STAGING_DIR
#
# By default, Spark w/ Yarn will use your home directory for staging
# files for a job.  This home directory must be accessible by all
# nodes.
#
# This may pose a problem if you are not using HDFS and your home
# directory is not NFS or network mounted.  Set this value to a
# network location as your staging directory.  Be sure to prefix this
# path the appropriate scheme, such as file://.
#
# This option is only available beginning in Spark 2.0.
#
# export SPARK_YARN_STAGING_DIR="file:///lustre/${USER}/sparkStaging/"

############################################################################
# Spark SparkPi Configuration
############################################################################

# SparkPi Slices
#
# Number of "slices" to parallelize in Pi estimation.  Generally
# speaking, more should lead to more accurate estimates.
#
# If not specified, equals number of nodes.
#
# export SPARK_SPARKPI_SLICES=4

############################################################################
# Spark SparkWordCount Configuration
############################################################################

# SparkWordCount File
#
# Specify the file to do the word count on.  Specify the scheme, such
# as hdfs:// or file://, appropriately.
#
# export SPARK_SPARKWORDCOUNT_FILE="/mywordcountfile"

# SparkWordCount Copy In File
#
# In some cases, a file must be copied in before it can be used.  Most
# notably, this can be the case if the file is not yet in HDFS.
#
# If specified below, the file will be copied to the location
# specified by SPARK_SPARKWORDCOUNT_FILE before the word count is
# executed.
#
# Specify the scheme appropriately.  At this moment, the schemes of
# file:// and hdfs:// are recognized for this option.
#
# Note that this is not required.  The file could be copied in any
# number of other ways, such as through a previous job or through a
# script specified via MAGPIE_PRE_JOB_RUN.
#
# export SPARK_SPARKWORDCOUNT_COPY_IN_FILE="/mywordcountfile"

############################################################################
# Run Job
############################################################################

srun --no-kill -W 0 $MAGPIE_SCRIPTS_HOME/magpie-check-inputs
if [ $? -ne 0 ]
then
    exit 1
fi
srun --no-kill -W 0 $MAGPIE_SCRIPTS_HOME/magpie-setup-core
if [ $? -ne 0 ]
then
    exit 1
fi
srun --no-kill -W 0 $MAGPIE_SCRIPTS_HOME/magpie-setup-projects
if [ $? -ne 0 ]
then
    exit 1
fi
srun --no-kill -W 0 $MAGPIE_SCRIPTS_HOME/magpie-setup-post
if [ $? -ne 0 ]
then
    exit 1
fi
srun --no-kill -W 0 $MAGPIE_SCRIPTS_HOME/magpie-pre-run
if [ $? -ne 0 ]
then
    exit 1
fi
srun --no-kill -W 0 $MAGPIE_SCRIPTS_HOME/magpie-run
srun --no-kill -W 0 $MAGPIE_SCRIPTS_HOME/magpie-cleanup
srun --no-kill -W 0 $MAGPIE_SCRIPTS_HOME/magpie-post-run
