import sys
import itertools

if __name__ == "__main__":
    # This itertools.groupby line here breaks when it comes across a different key
    # so that we get all of one key's values in an iteration. For example:
    #
    # hello\t1
    # hello\t1
    # blah\t1
    #
    # ... would yield two iterations where:
    #
    # key == "hello" and lines == ["hello\t1","hello\t1"]
    # key == "blah" and lines == ["blah\t1"]
    for key, lines in itertools.groupby(sys.stdin, lambda x: x.split("\t")[0]):
        sum = 0
        print key
        for line in lines:
            sum += int(line.split("\t")[1])
        print key + "\t" + str(sum)

