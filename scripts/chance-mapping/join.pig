/* 
 * ACHANCE lines look like this (join column in brackets):
 * [G-L1FB12-R1C1]\t0.590361445783133\t83.0
 *
 * ATEST lines look like this (join column in brackets):
 * 1\tstu_de2777346f\tUnit CTA1_01, Section CTA1_01-4\tL1FB12\t1\t[G-L1FB12-R1C1]\t0.889209324681567\t8322.0
 *
 * Andreas wanted to join on the chance ID column.
 */

-- The parameter to PigStorage() is your data's delimiter.
achance = LOAD '$ACHANCE' USING PigStorage('\t'); 
atest = LOAD '$ATEST' USING PigStorage('\t');

joined = JOIN achance BY $0, atest BY $5;

STORE joined INTO '$OUT' USING PigStorage('\t')


