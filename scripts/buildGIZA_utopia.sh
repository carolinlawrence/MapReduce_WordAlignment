if ! [ $# -eq 2 ]
  then
  echo "Usage: $0 mosesdir(your moses installation) mosesexternaldir(your giza++ installation)"
  exit 1
fi

moses=$1
giza=$2

mkdir -p ../systems/GIZA_utopia

#create language model
$moses/bin/lmplz -o 5 -S 40% <../corpora/utopia.zh >../systems/GIZA_utopia/lm.arpa
$moses/bin/build_binary ../systems/GIZA_utopia/lm.arpa ../systems/GIZA_utopia/lm.binary
rm ../systems/GIZA_utopia/lm.arpa 

#create translation model
$moses/scripts/training/train-model.perl -root-dir ../systems/GIZA_utopia/ --corpus ../corpora/utopia --f en --e zh -external-bin-dir $giza/ --lm 0:5:$(cd ../; pwd)/systems/GIZA_utopia/lm.binary:8 --giza-option m1=5,m2=0,m3=0,m4=1,mh=5

#run translation & score
$moses/bin/moses -f ../systems/GIZA_utopia/model/moses.ini <../corpora/utopiatest.en >../systems/GIZA_utopia/translated.txt
$moses/scripts/generic/multi-bleu.perl -lc ../corpora/utopiatest.zh <../systems/GIZA_utopia/translated.txt >../systems/GIZA_utopia/bleu.txt
