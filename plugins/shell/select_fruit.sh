FRUIT=$1
if [ $FRUIT == APPLE ]; then
	echo " You Selected Apple! "
elif [ $FRUIT == ORANGE ]; then
	echo " You Selected Orange! "
elif [ $FRUIT == GRAPE ]; then
	echo " You Selected Grape! "
else
	echo "You Selected other Fruit!" 
fi
