if ! [ $# -eq 2 ]
  then
  echo "Usage: $0 mosesdir(your moses installation) mosesexternaldir(your giza++ installation)"
  exit 1
fi

moses=$1
giza=$2

hadoop jar ../WordAlignment.jar de.heidelberg.cl.ap.ss13.helper.Main ../configs/utopia

mkdir ../systems
mkdir ../systems/MapReduce_utopia
mkdir ../systems/MapReduce_utopia/giza.en-zh
mkdir ../systems/MapReduce_utopia/giza.zh-en

cp ../utopia.viterbi ../systems/MapReduce_utopia/giza.zh-en/zh-en.A3.final
cp ../utopia.viterbi.reversed_order ../systems/MapReduce_utopia/giza.en-zh/en-zh.A3.final

gzip ../systems/MapReduce_utopia/giza.zh-en/zh-en.A3.final
gzip ../systems/MapReduce_utopia/giza.en-zh/en-zh.A3.final

rm ../systems/MapReduce_utopia/giza.zh-en/zh-en.A3.final
rm ../systems/MapReduce_utopia/giza.en-zh/en-zh.A3.final

#create language model
$moses/bin/lmplz -o 5 -S 40% <../corpora/utopia.zh >../systems/MapReduce_utopia/lm.arpa
$moses/bin/build_binary ../systems/MapReduce_utopia/lm.arpa ../systems/MapReduce_utopia/lm.binary
rm ../systems/MapReduce_utopia/lm.arpa 

#create translation model
$moses/scripts/training/train-model.perl -root-dir ../systems/MapReduce_utopia/ --corpus ../corpora/utopia --f en --e zh -external-bin-dir $giza/ --first-step 1 --last-step 1
$moses/scripts/training/train-model.perl -root-dir ../systems/MapReduce_utopia/ --corpus ../corpora/utopia --f en --e zh -external-bin-dir $giza/ --lm 0:5:$(cd ../; pwd)/systems/MapReduce_utopia/lm.binary:8 --first-step 3

#run translation & score
$moses/bin/moses -f ../systems/MapReduce_utopia/model/moses.ini <../corpora/utopiatest.en >../systems/MapReduce_utopia/translated.txt
$moses/scripts/generic/multi-bleu.perl -lc ../corpora/utopiatest.zh <../systems/MapReduce_utopia/translated.txt >../systems/MapReduce_utopia/bleu.txt
