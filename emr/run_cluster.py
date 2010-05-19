import os
import sys
import time
import commands
from optparse import OptionParser

def get_running_clusters():
    cmd = "elastic-mapreduce --list --active --nosteps"
    status, output = commands.getstatusoutput(cmd)
    if status != 0: raise Exception("`%s` exited with status %d and the following output:\n%s" % (cmd, status, output))
    
    if not output.strip(): return []

    clusters = []
    for line in output.strip().split("\n"):
        line = line.strip().split() # [id, status, master_url, name]
        if line[1] not in ("TERMINATED", "SHUTTING_DOWN", "FAILED"): clusters.append(tuple(line))
    return clusters

# Returns a cluster with the given name if it exists, otherwise returns None.    
def get_cluster(name):
    clusters = get_running_clusters()
    for cluster in clusters:
        if cluster[-1] == name: return cluster
    return None

def wait_until_cluster_starts(name):
    print "Waiting for cluster to start..."
    cluster = get_cluster(name)
    while cluster and cluster[1] != "WAITING": # WAITING means waiting for jobs to EMR
        print ".",; sys.stdout.flush() # Force the "I'm spinning" indicator to print.
        time.sleep(10)
        cluster = get_cluster(name)
    if not cluster: raise Exception("Spinning up the cluster failed.")
    print

def bootstrap_cluster(name, key_pair_file, s3_config):
    id, status, master_url, name = get_cluster(name)
    bootstrap_script = os.path.join(os.path.dirname(__file__), "bootstrap.py")

    print "Bootstrapping cluster..."

    # Ship keypair, GeoIP.dat, and bootstrap script to master node
    for file in (key_pair_file, s3_config, bootstrap_script):
        cmd = "scp -o StrictHostKeyChecking=no -i %s %s hadoop@%s:" % (key_pair_file, file, master_url)
        print cmd
        status, output = commands.getstatusoutput(cmd)
        if status != 0: raise Exception("`%s` exited with status %d and the following output:\n%s" % (cmd, status, output))

    # Run bootstrap.py on the master.
    cmd = "ssh -o StrictHostKeyChecking=no -i %s hadoop@%s 'python ./bootstrap.py %s'" % (key_pair_file, master_url, key_pair_file)
    print cmd
    status, output = commands.getstatusoutput(cmd)
    if status != 0: raise Exception("`%s` exited with status %d and the following output:\n%s" % (cmd, status, output))

if __name__ == '__main__':
    parser = OptionParser(usage="%prog [OPTIONS] NAME NUM_INSTANCES", description="Runs a cluster on EMR called NAME containing NUM_INSTANCES instances.")
    parser.set_defaults(instance_type="m1.small", log_uri="s3://emr-logs", key_pair_file=os.environ.get("EMR_KEYPAIR"), s3_config=os.path.join(os.path.expanduser("~"), ".s3cfg"))
    parser.add_option("-t", "--instance-type", dest="instance_type", help="The type of instances you want in the cluster. Defaults to m1.small")
    parser.add_option("-l", "--log-uri", dest="log_uri", help="The MapReduce log uri. Defaults to s3://emr-logs")
    parser.add_option("-k", "--key-pair-file", dest="key_pair_file", help="The EC2 keypair file. Defaults to the EMR_KEYPAIR environment variable, throwing an error if neither the option nor variable are set")
    parser.add_option("-s", "--s3-config", dest="s3_config", help="The path to .s3cfg for s3cmd. Defaults to ~/.s3cfg")
    options, args = parser.parse_args()

    if len(args) != 2:
        parser.print_help()
        parser.error("Two arguments required: NAME NUM_INSTANCES")

    if not options.key_pair_file:
        parser.print_help()
        parser.error("Path to key pair file required.")

    if not os.path.exists(options.s3_config):
        parser.print_help()
        parser.error("S3 config file does not exist at: %s" % options.s3_config)

    name, num_instances = args[0:2]

    if not get_cluster(name):
        cmd = "elastic-mapreduce --create --alive --name %s --num-instances %s --instance-type %s --log-uri %s" % (name, num_instances, options.instance_type, options.log_uri)
        print cmd
        status, output = commands.getstatusoutput(cmd)
        if status != 0: raise Exception("`%s` exited with status %d and the following output:\n%s" % (cmd, status, output))
        wait_until_cluster_starts(name)
        bootstrap_cluster(name, options.key_pair_file, options.s3_config)
        print "Done!"

    else: raise Exception("'%s' is already running." % name)    

    

