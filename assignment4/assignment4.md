I have only implemented single-source personalized page rank.  All programs should run and provide output with respect to only the first source node in the args list.  

As a side note, ExtractTopPersonalizedPageRankNodes runs locally (using etc/run.sh).  

I started implementing support for multiple-source personalized page rank, but I couldn't get a working implementation by the project deadline.  In particular, running 20 iterations of multiple MapReduce jobs ties up the VM for about 30 minutes.  The "extended" code is not included in my git repo because it doesn't compile currently.

Here is the output for personalized page rank with respect to two source nodes (note: this was computed using two different single-source runs)

Source: 9470136

0.38841 9470136

0.09422 7992850

0.08590 7891871

0.08067 10208640

0.06607 8747858

0.06607 9427340

0.03544 8702415

0.03182 8669492

0.02245 7970234

0.01692 8846238



Source: 9300650

0.44687 9300650

0.09108 10765057

0.08889 9074395

0.07597 10687744

0.07597 8832646

0.07597 9621997

0.01556 10448801

0.01511 9785148

0.01511 10369305

0.01511 11890488


