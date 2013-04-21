ExractHoulyCountsEgypt.pig:

A = load '/user/shared/tweets2011/tweets2011.txt' as (id:chararray, time:chararray, user:chararray, text:chararray);

B = filter A by (text matches '.*([Ee][Gg][Yy][Pp][Tt]|[Cc][Aa][Ii][Rr][Oo]).*');

B2 = foreach B generate SUBSTRING(time, 4, 13) as date_hour;

C = group B2 by date_hour; 

D = foreach C generate group as date_hour, COUNT(B2) as count;

store D into 'cnt-egypt';


ExtractHourlyCountsAll.pig:

A = load '/user/shared/tweets2011/tweets2011.txt' as (id:chararray, time:chararray, user:chararray, text:chararray);

B = foreach A generate SUBSTRING(time,4,13) as date_hour;

C = group B by date_hour; 

D = foreach C generate group as date_hour, COUNT(B) as count;

Z = filter D by 
(SUBSTRING(date_hour,0,6) == 'Jan 23') or (SUBSTRING(date_hour,0,6) == 'Jan 24') or
(SUBSTRING(date_hour,0,6) == 'Jan 25') or (SUBSTRING(date_hour,0,6) == 'Jan 26') or
(SUBSTRING(date_hour,0,6) == 'Jan 27') or (SUBSTRING(date_hour,0,6) == 'Jan 28') or
(SUBSTRING(date_hour,0,6) == 'Jan 29') or (SUBSTRING(date_hour,0,6) == 'Jan 30') or
(SUBSTRING(date_hour,0,6) == 'Jan 31') or (SUBSTRING(date_hour,0,6) == 'Feb 01') or 
(SUBSTRING(date_hour,0,6) == 'Feb 02') or (SUBSTRING(date_hour,0,6) == 'Feb 03') or 
(SUBSTRING(date_hour,0,6) == 'Feb 04') or (SUBSTRING(date_hour,0,6) == 'Feb 05') or 
(SUBSTRING(date_hour,0,6) == 'Feb 06') or (SUBSTRING(date_hour,0,6) == 'Feb 07') or 
(SUBSTRING(date_hour,0,6) == 'Feb 08');

store Z into 'cnt-all';

Grading
=======

Everything looks great!
Minor note: filtering *before* grouping will make your script more efficient.

Score: 25/25

-Jimmy
