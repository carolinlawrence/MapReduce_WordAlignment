if ! [ $# -eq 1 ]
  then
  echo "Usage: $0 mosesdir(your moses installation)"
  exit 1
fi

moses=$1

#get training data
wget http://www.statmt.org/wmt13/training-parallel-nc-v8.tgz
tar xzf training-parallel-nc-v8.tgz
rm training-parallel-nc-v8.tgz

head -n 5000 training/news-commentary-v8.de-en.de |
perl -pe 's/(.*?)/lc($1)/ge;' >../corpora/nc5000.lc.de

head -n 5000 training/news-commentary-v8.de-en.en |
perl -pe 's/(.*?)/lc($1)/ge;' >../corpora/nc5000.lc.en

head -n 20000 training/news-commentary-v8.de-en.de |
perl -pe 's/(.*?)/lc($1)/ge;' >../corpora/nc20000.lc.de

head -n 20000 training/news-commentary-v8.de-en.en |
perl -pe 's/(.*?)/lc($1)/ge;' >../corpora/nc20000.lc.en

$moses/scripts/training/clean-corpus-n.perl ../corpora/nc5000.lc de en ../corpora/nc5000.clean 0 80
$moses/scripts/training/clean-corpus-n.perl ../corpora/nc20000.lc de en ../corpora/nc20000.clean 0 80

rm ../corpora/nc5000.lc.de
rm ../corpora/nc5000.lc.en
rm ../corpora/nc20000.lc.de
rm ../corpora/nc20000.lc.en

$moses/scripts/tokenizer/tokenizer.perl -l de <../corpora/nc5000.clean.de >../corpora/nc5000.de
$moses/scripts/tokenizer/tokenizer.perl -l en <../corpora/nc5000.clean.en >../corpora/nc5000.en
$moses/scripts/tokenizer/tokenizer.perl -l de <../corpora/nc20000.clean.de >../corpora/nc20000.de
$moses/scripts/tokenizer/tokenizer.perl -l en <../corpora/nc20000.clean.en >../corpora/nc20000.en

rm ../corpora/nc5000.clean.de
rm ../corpora/nc5000.clean.en
rm ../corpora/nc20000.clean.de
rm ../corpora/nc20000.clean.en

rm -rf training

#get test set
wget http://www.statmt.org/wmt13/test.tgz
tar xzf test.tgz
rm test.tgz

cat test/newstest2013-src.en.sgm |
perl -pe 's/<.*?>//ge;' |
perl -pe 's/^\n//ge;' |
perl -pe 's/(.*?)/lc($1)/ge;' >../corpora/nctest.lc.en

cat test/newstest2013-src.de.sgm |
perl -pe 's/<.*?>//ge;' |
perl -pe 's/^\n//ge;' |
perl -pe 's/(.*?)/lc($1)/ge;' >../corpora/nctest.lc.de

$moses/scripts/training/clean-corpus-n.perl ../corpora/nctest.lc de en ../corpora/nctest.clean 0 80

rm ../corpora/nctest.lc.de
rm ../corpora/nctest.lc.en

$moses/scripts/tokenizer/tokenizer.perl -l de <../corpora/nctest.clean.de >../corpora/nctest.de
$moses/scripts/tokenizer/tokenizer.perl -l en <../corpora/nctest.clean.en >../corpora/nctest.en

rm ../corpora/nctest.clean.de
rm ../corpora/nctest.clean.en

rm -rf test
