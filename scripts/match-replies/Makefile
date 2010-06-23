.PHONY: clean
.PHONY: install

all: udfs.jar

udfs.jar: udfs
	cd udfs && make
	jar -cf udfs.jar udfs

clean:
	rm -f udfs.jar
	cd udfs && make clean