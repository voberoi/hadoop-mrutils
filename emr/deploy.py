# deploy.py
# This script takes a (required) directory containing pig scripts and a (optional) directory on
# s3 to deploy them to. REGISTER lines are replaced with s3 paths to deployed jar files.

import os
import re
import sys
import glob
import shutil

def prepare_pig_script(path, s3_dir):
    script = open(path).read()
    
    # Replace all jar files in REGISTERs with s3 paths.
    jars = []
    for match in re.finditer("\S+\.jar;", script): jars.append(match)
    jars.sort(lambda x,y: y.start() - x.start()) # Replace backwards

    for jar in jars:
        jar_path = script[jar.start():jar.end()]
        jar_path = os.path.join("s3://%s", os.path.basename(jar_path))
        script = script[:jar.start()] + jar_path + script[jar.end():]
        
    open(path, "w").write(script)

if __name__ == '__main__':
    deploy_dir, s3_dir = sys.argv[1:3]

    for root, dirs, files in os.walk(deploy_dir):
        for file in files:
            if file.endswith(".pig"): prepare_pig_script(os.path.join(root, file))
    
    # Sync scripts and jar files.
    cmd = "s3cmd sync %s s3://%s/" % (deploy_dir, s3_dir)
    os.system(cmd)
