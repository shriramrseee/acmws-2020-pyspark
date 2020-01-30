```
m = sc.textFile("hdfs:///user/simmhan/ml/small/movies.csv").cache()
r = sc.textFile("hdfs:///user/simmhan/ml/small/ratings.csv").cache()
t = sc.textFile("hdfs:///user/simmhan/ml/small/tags.csv").cache()
l = sc.textFile("hdfs:///user/simmhan/ml/small/links.csv").cache()

#####################################################################################
# How many distinct users have tagged movies? 

tu = t.map(lambda l : l.split(",")[0]).distinct()
print 'Number of Users: ', tu.count()-1

#####################################################################################
# How many users have both given ratings and tagged movies?

ru = r.map(lambda l : l.split(",")[0]).distinct()
print 'Number of Users: ', tu.intersection(ru).count() - 1

#####################################################################################
# What are the most number of genres assigned to any movie?

# Unicode reader
# Reference: https://docs.python.org/2/library/csv.html#examples
import csv

def unicode_csv_reader(unicode_csv_data, dialect=csv.excel, **kwargs):
    csv_reader = csv.reader(utf_8_encoder(unicode_csv_data), dialect=dialect, **kwargs)
    for row in csv_reader:
        yield [unicode(cell, 'utf-8') for cell in row]

def utf_8_encoder(unicode_csv_data):
    for line in unicode_csv_data:
        yield line.encode('utf-8')

mp = m.map(lambda l: tuple(unicode_csv_reader([l,]))[0])
mp = mp.filter(lambda l: l[0] != u'movieId').map(lambda l: (len(l[2].split('|')), l))
mc = mp.map(lambda l: l[0]).max()
mcm = mp.lookup(mc)
print mc, mcm

#####################################################################################
# Which are the genres with the most and least number of movies?


mp = m.map(lambda l: tuple(unicode_csv_reader([l,]))[0])
mgs = mp.flatMap(lambda l : l[2].split("|")).map(lambda g : (g, 1)).reduceByKey(lambda a, b: a + b)
msg = mgs.map(lambda (g,s) : (s,g))
gmin = msg.min()
gmax = msg.max()
print 'Genre with min films is',gmin[1],'with count',gmin[0]
print 'Genre with max films is',gmax[1],'with count',gmax[0]
```
