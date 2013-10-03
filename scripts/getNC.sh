#get training data
wget http://www.statmt.org/wmt13/training-parallel-nc-v8.tgz
tar xzf training-parallel-nc-v8.tgz
rm training-parallel-nc-v8.tgz

head -n 5000 training/news-commentary-v8.de-en.de |
perl -pe 's/(.*?)/lc($1)/ge;' >../corpora/nc5000.de

head -n 5000 training/news-commentary-v8.de-en.en |
perl -pe 's/(.*?)/lc($1)/ge;' >../corpora/nc5000.en

head -n 20000 training/news-commentary-v8.de-en.de |
perl -pe 's/(.*?)/lc($1)/ge;' >../corpora/nc20000.de

head -n 20000 training/news-commentary-v8.de-en.en |
perl -pe 's/(.*?)/lc($1)/ge;' >../corpora/nc20000.en

rm -rf training
