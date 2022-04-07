sudo apt-get update
sudo apt-get -y install python3 python3-pip git
pip install dask[complete]
pip install pyarrow

git clone https://github.com/Maciekkucharski/BGT_LAB_4.git

mkdir ~/data1, ~/data2, ~/data3, ~/data4
gsutil -m cp gs://pjwstk-bigdata/*.parquet ./data1/.
sudo cp -r ~/data1 ~/data2
sudo cp -r ~/data1 ~/data3
sudo cp -r ~/data1 ~/data4