# Hadoop MR Utils #

### What is this? ###

This is a collection of my helper scripts for running jobs on Elastic MapReduce
and example code to get started with Hadoop streaming and Pig jobs. I put all 
these together for a workshop at Noisebridge's Machine Learning meetup, so
they're rather Noisebridge-specific at times.

Feel free to use and repurpose all this code for whatever you want.

### What's in here? ###

* `emr/` contains a bunch of helper scripts to spin up clusters, deploy code
to S3, and run jobs on Elastic MapReduce.
* `scripts/` contains a bunch of example code to demonstrate writing jobs using
Hadoop streaming and Pig.

### AWS/EMR Setup ###

Setup instructions for AWS and Elastic MapReduce are  in `emr/setup-instructions/.

### Hadoop Setup ###

If you want to set up Hadoop locally (not necessary to understand or run these
examples on Elastic MapReduce), I'd suggest using Cloudera's RPM and deb
packages on a RedHat or Debian-based Linux distribution. It'll save you
enormous hassle.

### Pig Setup ###

It'll be useful to set up Pig version 0.6 or less to run and test your Pig
code locally over smaller samples of your data. Pig 0.7 onwards requires
Hadoop to run locally. While there are known discrepancies between running
Pig version <= 0.6 in local mode and on a Hadoop cluster, they aren't likely
to crop up in these examples and it'll be much, much simpler to get started.

Download Pig 0.6 from here: http://hadoop.apache.org/pig/releases.html (click
on "Download a release now!" and pick one of the mirrors).

Extract the contents somewhere (mine are in /home/voberoi/pig) and add the
following to your ~/.bash_rc or ~/.bash_profile:

export PATH=/home/voberoi/pig/bin:$PATH

You'll also need to set the JAVA_HOME variable. Google around to figure out
what your JAVA_HOME is and add the following to your ~/.bash_rc or 
~/bash_profile:

export JAVA_HOME=/path/to/java/home

When you want to run a Pig program locally, always include `-x local` as an 
argument. For example:

`pig -x local -param INPUT=/some/data -param OUTPUT=/output/data myscript.pig`

### Hadoop Streaming ###

Hadoop allows you to write your mappers and reducers in any language. Your 
mappers and reducers must read input on STDIN and write to STDOUT. Check out
the word-count Python examples in `scripts/word-count`.

The nice thing about this is that it's remarkably easy to test locally without
setting up a Hadoop cluster. To 'simulate MapReduce' all you need to do is this:

`cat data | my_mapper | sort | my_reducer > output`
