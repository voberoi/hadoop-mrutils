/*
 * word-count.pig
 *
 * A Pig script that counts the frequency of all words in $INPUT.
 */

REGISTER ./udfs.jar

lines = LOAD '$INPUT' USING PigStorage();
words = FOREACH lines GENERATE FLATTEN(TOKENIZE($0));
words_trimmed = FOREACH words GENERATE udfs.Trim($0);
words_grouped = GROUP words_trimmed BY $0;
counts = FOREACH words_grouped GENERATE group, COUNT($1);
STORE counts INTO '$OUTPUT';
