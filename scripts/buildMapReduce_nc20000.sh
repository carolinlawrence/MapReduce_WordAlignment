if ! [ $# -eq 2 ]
  then
  echo "Usage: $0 mosesdir(your moses installation) mosesexternaldir(your giza++ installation)"
  exit 1
fi

moses=$1
giza=$2

hadoop jar ../WordAlignment.jar de.heidelberg.cl.ap.ss13.helper.Main ../configs/nc20000
hadoop jar ../WordAlignment.jar de.heidelberg.cl.ap.ss13.helper.Main ../configs/nc20000_reverse

mkdir ../systems
mkdir ../systems/MapReduce_nc20000
mkdir ../systems/MapReduce_nc20000/giza.de-en
mkdir ../systems/MapReduce_nc20000/giza.en-de

cp ../nc20000.viterbi ../systems/MapReduce_nc20000/giza.en-de/en-de.A3.final
cp ../nc20000.viterbi.reversed_order ../systems/MapReduce_nc20000/giza.de-en/de-en.A3.final

gzip ../systems/MapReduce_nc20000/giza.en-de/en-de.A3.final
gzip ../systems/MapReduce_nc20000/giza.de-en/de-en.A3.final

rm ../systems/MapReduce_nc20000/giza.en-de/en-de.A3.final
rm ../systems/MapReduce_nc20000/giza.de-en/de-en.A3.final

#create language model
$moses/bin/lmplz -o 5 -S 40% <../corpora/nc20000.en >../systems/MapReduce_nc20000/lm.arpa
$moses/bin/build_binary ../systems/MapReduce_nc20000/lm.arpa ../systems/MapReduce_nc20000/lm.binary
rm ../systems/MapReduce_nc20000/lm.arpa 

#create translation model
$moses/scripts/training/train-model.perl -root-dir ../systems/MapReduce_nc20000/ --corpus ../corpora/nc20000 --f de --e en -external-bin-dir $giza/ --first-step 1 --last-step 1
$moses/scripts/training/train-model.perl -root-dir ../systems/MapReduce_nc20000/ --corpus ../corpora/nc20000 --f de --e en -external-bin-dir $giza/ --lm 0:5:$(cd ../; pwd)/systems/MapReduce_nc20000/lm.binary:8 --first-step 3

#run translation & score
$moses/bin/moses -f ../systems/MapReduce_nc20000/model/moses.ini <../corpora/nctest.de >../systems/MapReduce_nc20000/translated.txt
$moses/scripts/generic/multi-bleu.perl -lc ../corpora/nctest.en <../systems/MapReduce_nc20000/translated.txt >../systems/MapReduce_nc20000/bleu.txt
