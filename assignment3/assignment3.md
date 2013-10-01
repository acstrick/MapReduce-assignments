Question 1

After compression, the data is 9.2 MB and the index is 3.2 KB.

Grading
=======

Everything looks fine---good job!

As a side note, though, using a Pair object to represent the postings
list makes the index larger than it needs to be: you can actually fit
the *df* in the `BytesWritable` also.


