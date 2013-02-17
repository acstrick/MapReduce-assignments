Assignment 2

Question 0

I first counted the lines in the dataset using a simple MapReduce job, LineCount.java, where the mapper emits an IntWritable with a value of 1 for every line and the reducer sums all counts.  Though a MapReduce is clearly not needed for counting lines in a file, I thought it would be good practice.  

The “pairs” implementation consists of one MapReduce job where the mapper reads lines and emits a (<PairOfStrings>,<FloatWritable>) pair with a count of 1 for each occurrence of (x,y) and (x, *).  A combiner aggregates each mapper’s output, summing like pairs, and the reducer does the same.  Pairs with a ‘*’ right element are processed first.  Both emit (<PairofStrings>,<FloatWritable>) pairs.

The “stripes” implementation consists of a MapReduce job where the mapper reads lines and emits a (<Text>,<HMapSIW>) pair for each unique word in a line and the map associated with that word.  In addition, a value of ‘*’ is added to each word’s map to represent the its individual presence in the line.   A combiner aggregates each mapper’s output, aggregating maps based on key.  Both emit a (<Text>,<HMapSIW>) pairs. 

The above MapReduce jobs just provide counts of given words and pairs (per line) from the dataset.  I used a Python script to calculate PMI values based on MapReduce output files.  While this probably does not meet the intention of the assignment, I could not think of a way to organize the data through a secondary MapReduce job (or by adjusting the above approaches) to allow calculations from terms anywhere in the dataset.  Some Python scription, however, made the job straightforward.

Question 1

LineCount.java:  MR job to count lines in corpus, runtime = 128.64 seconds.

PairsPMI.java:  MR job to count P(x,y) and P(x), runtime = 902.28 seconds.

StripesPMI.java:  MR job to count P(x,y) and P(x), runtime = 344.59 seconds.

The final analysis via python took about a minute to run.

Question 2

PairsPMI.java (combiners disabled):  MR job to count P(x,y) and P(x), runtime = 944.21 seconds.

StripesPMI.java (combiners disabled):  MR job to count P(x,y) and P(x), runtime = 320.44 seconds.

Question 3

There are 241,323 PMI pairs (though this accounts for both (x,y) and (y,x)).

Question 4

The pair (‘exit’, ‘and’) has the highest PMI at -14.76.  An inspection of the Shakespeare + Bible dataset shows that the term “exit” is used on a scene change in the Shakespeare plays.  The term “and” appears to be used frequently throughout both parts of the dataset.  Other commonly used terms such as “enter”, “act”, and “exeunt” from the Shakespeare plays account for many of the pairs with highest PMI values.

Question 5

The following are the words with highest PMI values associated with “cloud”: (‘love’, ‘before’), -13.2; (‘love’,’up’), -13.16; and (‘love’,’day’), -13.1.

The following are the words with highest PMI values associated with “love”: (‘cloud’,’my’), -12.04; (‘cloud’,’I’), -11.97; and (‘cloud’,’his’), -11.79.


