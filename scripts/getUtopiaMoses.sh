if ! [ $# -eq 1 ]
  then
  echo "Usage: $0 mosesdir(your moses installation)"
  exit 1
fi

moses=$1

#get training data
wget https://raw.github.com/wlin12/SMMTT/master/data/parallel/microtopia.en-zh

cat microtopia.en-zh |
perl -pe 's/(.*?) \|\|\| .*?\n/$1\n/g;' |
perl -pe 's/(.*?)/lc($1)/ge;' > ../corpora/utopia.lc.en

cat microtopia.en-zh |
perl -pe 's/.*? \|\|\| (.*?\n)/$1/g;' |
perl -pe 's/([^\p{IsAlnum}])/ $1 /g;' > ../corpora/utopia.zh

rm microtopia.en-zh

$moses/scripts/tokenizer/tokenizer.perl -l en <../corpora/utopia.lc.en >../corpora/utopia.en

rm ../corpora/utopia.lc.en

#get test set
wget https://raw.github.com/wlin12/SMMTT/master/data/parallel_test/microtopia.en-zh

cat microtopia.en-zh |
perl -pe 's/^(.*?) \|\|\| .*?\n/$1\n/g;' |
perl -pe 's/(.*?)/lc($1)/ge;' > ../corpora/utopiatest.lc.en

cat microtopia.en-zh |
perl -pe 's/.*? \|\|\| (.*?\n)/$1/g;' |
perl -pe 's/([^\p{IsAlnum}])/ $1 /g;' > ../corpora/utopiatest.zh

rm microtopia.en-zh

$moses/scripts/tokenizer/tokenizer.perl -l en <../corpora/utopiatest.lc.en >../corpora/utopiatest.en

rm ../corpora/utopiatest.lc.en


