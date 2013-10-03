#get training data
wget https://raw.github.com/wlin12/SMMTT/master/data/parallel/microtopia.en-zh

cat microtopia.en-zh |
perl -pe 's/(.*?) \|\|\| .*?\n/$1\n/g;' |
perl -pe 's/(.*?)/lc($1)/ge;' > ../corpora/utopia.en

cat microtopia.en-zh |
perl -pe 's/.*? \|\|\| (.*?\n)/$1/g;' > ../corpora/utopia.zh

rm microtopia.en-zh
