# deploy.py
# This script takes a directory containing scripts and a directory on s3 to deploy them
# to. REGISTER lines are replaced with s3 paths to deployed jar files.

import os
import re
import sys
import glob
import shutil

TMP_SUFFIX = ".deploytmp"

def prepare_pig_script(path, s3_dir):
    script = open(path).read()
    os.system("mv %s %s" % (path, path + TMP_SUFFIX))

    # Replace all jar files in REGISTERs with s3 paths.
    jars = []
    for match in re.finditer("\S+\.jar;", script): jars.append(match)
    jars.sort(lambda x,y: y.start() - x.start()) # Replace backwards

    for jar in jars:
        jar_path = script[jar.start():jar.end()]
        jar_path = os.path.join("s3://", os.path.join(s3_dir, os.path.dirname(path), os.path.basename(jar_path)))
        script = script[:jar.start()] + jar_path + script[jar.end():]
        
    open(path, "w").write(script)

if __name__ == '__main__':
    scripts_dir, s3_dir = sys.argv[1:3]

    for root, dirs, files in os.walk(scripts_dir):
        for file in files:
             if file.endswith(".pig"): prepare_pig_script(os.path.join(root, file), s3_dir)

    # Sync scripts and jar files.
    cmd = "s3cmd sync %s s3://%s/" % (scripts_dir, s3_dir)
    os.system(cmd)

    for root, dirs, files in os.walk(scripts_dir):
        for file in files:
            if file.endswith(TMP_SUFFIX):
                cmd = "mv %s %s" % (os.path.join(root, file), os.path.join(root,file[:len(TMP_SUFFIX)*-1]))
                os.system(cmd)
