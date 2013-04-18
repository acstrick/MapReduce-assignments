A = load '/user/shared/tweets2011/tweets2011.txt' as (id:chararray, time:chararray, user:chararray, text:chararray);
B = filter A by (text matches '.*([Ee][Gg][Yy][Pp][Tt]|[Cc][Aa][Ii][Rr][Oo]).*');
B2 = foreach B generate SUBSTRING(time, 4, 13) as date_hour;
C = group B2 by date_hour; 
D = foreach C generate group as date_hour, COUNT(B2) as count;

store D into 'cnt-egypt';