import sys
from optparse import OptionParser

from pig import pig, get_jobflow_id

if __name__ == "__main__":
    parser = OptionParser(usage="%prog [-n JOBFLOW_NAME] [-j JOBFLOW_ID] SCRIPT PARAMS", description="Runs a Pig job in the Elastic MapReduce jobflow with the given JOBFLOW_NAME or JOBFLOW_ID. If both are specified, uses JOBFLOW_ID. If neither are specified, throws an error. PARAMS should be '='-delimited Pig params only (i.e. ... LOGS=logs/20091202 OUT=s3://my.output/job-output")
    parser.add_option("-j", "--jobflow-id", dest="jobflow_id", help="Runs SCRIPT in the jobflow with this ID.")
    parser.add_option("-n", "--jobflow-name", dest="jobflow_name", help="Runs SCRIPT in the jobflow with this name.")
    options, args = parser.parse_args()

    script = args[0]
    params = args[1:]
    params = dict([param.split('=') for param in params])

    if options.jobflow_id:
        pig(script, option.jobflow_id, **params)
    elif options.jobflow_name:
        pig(script, get_jobflow_id(options.jobflow_name), **params)
    else:
        parser.print_help()
        parser.error("Either -n or -j have to be specified.")
