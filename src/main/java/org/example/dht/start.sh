for i in {10101..10110}
do
  if [ $i -eq 1 ]; then
    echo "first node $i"
    java Chord $i &
  else
    echo "cur node $i"
    java Chord $i "10.190.71.239" 10101 &
  fi
done

