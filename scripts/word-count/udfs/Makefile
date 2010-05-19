CLASSES=Trim.class

.PHONY: clean

%.class: %.java
	javac -cp $(PIGDIR)/pig.jar $<

all: $(CLASSES)

clean:
	rm -f *.class