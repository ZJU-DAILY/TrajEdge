ID=""
for (( i=0; i<8; i++ ))
do
    ID+=$(shuf -n 1 "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789")
done
echo $ID

docker cp ./TrajEdge-stream/target/TrajEdge-stream-1.0-SNAPSHOT.jar nimbus:/opt/storm/lib

docker exec -it nimbus storm jar /opt/storm/lib/TrajEdge-stream-1.0-SNAPSHOT.jar org.example.TrajectoryUploadTopology $ID true 1000
