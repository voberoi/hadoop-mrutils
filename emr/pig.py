import os
import commands
import re
import random

PIG_SCRIPTS = "s3://scripts"
PIG_BIN = "elastic-mapreduce --pig-script --step-action CONTINUE"

def pig(script, jobflow_id, **scriptparams):
	params = ' '.join(["--args --param,%s=%s" % (k,v) for k,v in scriptparams.items()])
        jobflow_id = "--jobflow %s" % jobflow_id
	cmd = PIG_BIN + " " + jobflow_id + " " + params + " --args " + os.path.join(PIG_SCRIPTS, script)
	print "Running pig job:"
	print "%s" % cmd

        # Try to kick off job.
	status, output = commands.getstatusoutput(cmd)
	if status != 0: 
                raise EMRError(output)

	return status

def get_jobflow_id(jobflow_name):
        cmd = "elastic-mapreduce --active --list --nosteps"
        status, output = commands.getstatusoutput(cmd)
        
        jobflows = {}
        for line in output.strip().split("\n"):
                line = line.strip().split()
                if line[1] not in ("TERMINATED", "SHUTTING_DOWN"):
                        id, status, master_url, name = line
                        if name not in jobflows: jobflows[name] = []
                        jobflows[name].append(id)
        
        # Complain if the jobflow name doesn't exist or if multiple jobflows
        # with the same name exist.
        if jobflow_name not in jobflows: raise EMRError("Jobflow with name %s doesn't exist." % jobflow_name)
        if len(jobflows[jobflow_name]) > 1: raise EMRError("Multiple jobflows with the name %s exist." % jobflow_name)
        
        return jobflows[jobflow_name][0]

class EMRError(Exception):
        pass

class PigError(Exception):
        pass

