if ! [ $# -eq 2 ]
  then
  echo "Usage: $0 mosesdir(your moses installation) mosesexternaldir(your giza++ installation)"
  exit 1
fi

moses=$1
giza=$2

mkdir ../systems
mkdir ../systems/GIZA_nc20000

#create language model
$moses/bin/lmplz -o 5 -S 40% <../corpora/nc20000.en >../systems/GIZA_nc20000/lm.arpa
$moses/bin/build_binary ../systems/GIZA_nc20000/lm.arpa ../systems/GIZA_nc20000/lm.binary
rm ../systems/GIZA_nc20000/lm.arpa 

#create translation model
$moses/scripts/training/train-model.perl -root-dir ../systems/GIZA_nc20000/ --corpus ../corpora/nc20000 --f de --e en -external-bin-dir $giza/ --lm 0:5:$(cd ../; pwd)/systems/GIZA_nc20000/lm.binary:8 --giza-option m1=5,m2=0,m3=0,m4=1,mh=5

#run translation & score
$moses/bin/moses -f ../systems/GIZA_nc20000/model/moses.ini <../corpora/nctest.de >../systems/GIZA_nc20000/translated.txt
$moses/scripts/generic/multi-bleu.perl -lc ../corpora/nctest.en <../systems/GIZA_nc20000/translated.txt >../systems/GIZA_nc20000/bleu.txt
